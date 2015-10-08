package socketclient
import (
    "fmt"
//    "os"
    "testing"
    "time"
//    "runtime/pprof"
    "sync/atomic"
    "github.com/stretchr/testify/assert"
)

func TestPC(t *testing.T) {
    /*f, err := os.Create("testprof")
    if err != nil {
        panic(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()*/

    PC, err := NewSocketClient("127.0.0.1:8124","test-service")
    strt := time.Now()
    if assert.NoError(t, err) {
        var pong string
        err := PC.Exec("ping", "", &pong)
        assert.Equal(t, pong, "pong")
        assert.Equal(t, err, nil)
        for x:= 0; x<10000; x++ {
            var test, test2 float64
            test = float64(x)
            err := PC.Exec("mul10", test, &test2)
            //fmt.Printf("%d %#v %#v %#v\n", x, test, test2 , err)
            assert.Nil(t, err)
            assert.Equal(t, int(test2), int(test*10), "they should be equal")
            //assert.NoError(t, err)
            //assert.NotNil(t, resp)
        }
        fmt.Printf("loop end\n")
    }
    fmt.Printf("%+v\n", time.Since(strt))
    PC.Close()
}

func TestPerf(t *testing.T) {
    PC, err := NewSocketClient("127.0.0.1:8124","test-service")
    assert.NoError(t, err)
    cnt := 100
    shit := make(chan int)
    var doen int64 = 0
    strt := time.Now()
    for x :=0; x<cnt; x++ {
        go func (x int) {
            y:=0
            for y=0;y<1000; y++ {
                var req float64 = float64(y)
                var res float64
                for {
                    err := PC.Exec("mul10", req, &res)
                    if err == ErrReconnect {
                        fmt.Printf("%d %d Reconnect\n", x, y)
                    } else {
                        break
                    }
                }
                assert.Nil(t, err)
                assert.Equal(t, int(req)*10, int(res))
                atomic.AddInt64(&doen,1)
            }
            shit <- 1
            //fmt.Printf("%d %d\n", x, y)
        }(x)
    }
    for z :=0; z<cnt; z++ {
        <- shit
    }
    fmt.Printf("%+v\n", time.Since(strt))
    fmt.Printf("Reqdoen: %d\n", doen)
    PC.Close()
}
