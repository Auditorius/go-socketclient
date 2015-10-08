package socketclient

import (
        "bufio"
        "encoding/json"
        "errors"
        "bytes"
        "fmt"
        "sync/atomic"
        "net"
        "log"
        "runtime"
        "sync"
        "strconv"
        "time"
)

type SocketRequest struct {
    request []byte
    result interface{}
    id int64
    sent bool
    responseChan chan error
}

const (
    SocketReconnecting int32 = iota
    SocketOpen
    SocketClosed
)

type SocketClient struct {
    state int32
    conn *net.TCPConn
    reader *bufio.Reader
    address *net.TCPAddr
    namespace string
    reqid int64
    changes sync.Mutex
    queue map[int64]*SocketRequest
    writeQueue chan *SocketRequest
    writeQueueHead *SocketRequest
}

type SocketRemoteError struct {
    RemoteError interface{}
}

func (sre *SocketRemoteError) Error() string {
    return fmt.Sprintf("SocketRemoteError: %#v", sre.RemoteError)
}

var (
    ErrReconnect = errors.New("Socket is reconnecting")
    ErrShutdown = errors.New("Socket is shutting down")
)

func NewSocketClient(addr string, namespace string) (*SocketClient, error) {
    address, err := net.ResolveTCPAddr("tcp",addr)
    if err != nil {
        return nil, err
    }
    pc := &SocketClient {
        address: address,
        state: SocketReconnecting,
        queue: make(map[int64]*SocketRequest),
        writeQueue: make(chan *SocketRequest,128),
        writeQueueHead: nil,
        namespace: namespace,
    }
    runtime.SetFinalizer(pc, (*SocketClient).Close)
    go pc.writerRoutine()
    go pc.readerRoutine()
    return pc, nil
}

func (pc *SocketClient) Close() {
    pc.changes.Lock()
    defer pc.changes.Unlock()

    if atomic.SwapInt32(&pc.state, SocketClosed) != SocketClosed {
        close(pc.writeQueue)
    }
}

func (pc *SocketClient) writerRoutine() {
    writerloop: for {
        switch atomic.LoadInt32(&pc.state) {
            case SocketClosed:
                pc.changes.Lock()
                for _,v := range pc.queue {
                    v.responseChan <- ErrShutdown
                }
                pc.changes.Unlock()
                break writerloop;
            case SocketOpen:
                //log.Printf("[SocketClient] Writer state: Open")
                request := pc.writeQueueHead
                if request == nil {
                    select {
                        case request = <- pc.writeQueue:
                            pc.writeQueueHead = request
                            if request == nil {
                                continue
                            }
                        case <- time.After(1*time.Second):
                            continue
                    }
                }
                _, err := pc.conn.Write(request.request)
                if err != nil {
                    log.Printf("[SocketClient] Write failed: %s", err.Error())
                    atomic.CompareAndSwapInt32(&pc.state, SocketOpen, SocketReconnecting)
                } else {
                    //log.Printf("[SocketClient] Wrote: %s", request.request)
                    pc.writeQueueHead = nil
                    request.sent = true
                }
            case SocketReconnecting:
                //log.Printf("[SocketClient] Writer state: Recon")
                pc.changes.Lock()
                conn, err := net.DialTCP(pc.address.Network(), nil, pc.address)
                if err == nil {
                    pc.reader = bufio.NewReader(conn)
                    pc.conn = conn
                    sentshit := make([]int64, 0, 32)
                    for k,v := range pc.queue {
                        if v.sent {
                            v.responseChan <- ErrReconnect
                            sentshit = append(sentshit, k)
                        }
                    }
                    for _,k := range sentshit {
                        delete(pc.queue, k)
                    }
                    atomic.CompareAndSwapInt32(&pc.state, SocketReconnecting, SocketOpen)
                } else {
                    log.Printf("[SocketClient] Reconnect failed: %s", err.Error())
                    time.Sleep(5*time.Second)
                }
                pc.changes.Unlock()
        }
    }
}

func (pc *SocketClient) readerRoutine() {
    readerloop: for {
        switch atomic.LoadInt32(&pc.state) {
            case SocketClosed:
                break readerloop;
            case SocketReconnecting:
                time.Sleep(time.Second/10)
            case SocketOpen:
                pc.changes.Lock()
                reader := pc.reader
                pc.changes.Unlock()
                line, err := reader.ReadBytes('\n')
                //log.Printf("[SocketClient] Recv: %s", line)
                if err != nil {
                    atomic.CompareAndSwapInt32(&pc.state, SocketOpen, SocketReconnecting)
                } else {
                    err = pc.response(line)
                    if err != nil {
                        log.Printf("[SocketClient] invalid response (%s): %s", err.Error(), line)
                    }
                }
        }
    }
}

func (pc *SocketClient) response(resp []byte) error {
    dec := json.NewDecoder(bytes.NewReader(resp))

    t, err := dec.Token()
    if err != nil {
        return err
    }
    if t != json.Delim('[') {
        return errors.New("line not starting with [")
    }
    var reqidf float64

    if !dec.More() {
        return errors.New("response abruptly before request id")
    }

    err = dec.Decode(&reqidf)
    if err != nil {
        return err
    }
    reqid := int64(reqidf)

    if !dec.More() {
        return errors.New("response abruptly before error object")
    }

    var resErr interface{}

    err = dec.Decode(&resErr)
    if err !=nil {
        return err
    }

    if !dec.More() {
        return errors.New("response abruptly before result object")
    }

    pc.changes.Lock()
    defer pc.changes.Unlock()

    request, ok := pc.queue[reqid]
    if !ok {
        return errors.New("Unknown request id "+strconv.FormatInt(reqid, 10))
    }

    defer delete(pc.queue, reqid)

    if resErr == nil {
        err = dec.Decode(request.result)
        request.responseChan <- err
        return err
    }
    request.responseChan <- &SocketRemoteError{resErr}
    return nil
}

func (pc *SocketClient) Exec(method string, args interface{}, result interface{}) error {
    reqid := atomic.AddInt64(&pc.reqid, 1)
    requestArray := []interface{}{
        float64(reqid),
        pc.namespace+":"+method,
        args,
        true,
    }
    requestBytes, err := json.Marshal(requestArray)
    if err != nil {
        return err
    }
    request := &SocketRequest{
        id: reqid,
        request: append(requestBytes, '\n'),
        result: result,
        responseChan: make(chan error),
    }
    pc.changes.Lock()
        pc.queue[reqid] = request
        pc.writeQueue <- request
    pc.changes.Unlock()
    return <- request.responseChan
}
