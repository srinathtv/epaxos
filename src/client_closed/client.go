package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"math/rand"
	"net"
	"net/rpc"
	"runtime"
	"state"
	"time"
  "os"
  "path/filepath"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var numClients *int = flag.Int("q", 1, "Number of client threads.  Defaults to 1.")
//var duration *int = flag.Int("d", 60, "Duration of the experiment in seconds.  Defaults to 60.")
var clientIdStartIdx *int = flag.Int("clientIdStartIdx", 0, "Starting index for the client. Defaults to 0.")
var numOutstandingReqs *int = flag.Int("numOutstandingReqs", 1, "Number of outstanding requests. Defaults to 1.")


func main() {
	flag.Parse()
  
  fname := filepath.Join(os.TempDir(), "stdout")
  fmt.Println("stdout is now set to", fname)
  old := os.Stdout
  temp, _ := os.Create(fname)
  os.Stdout = temp

  runtime.GOMAXPROCS(*numClients+1)
	for i:=0; i<*numClients; i++ {
    go run_one_client(*clientIdStartIdx + i, *numClients, *masterAddr, *masterPort)
  }

  time.Sleep(60 * time.Second)
  
  temp.Close()
  os.Stdout = old


	fmt.Printf("[[DONE]]\n")
}

func run_one_client(clientId int, numClients int, masterAddr string, masterPort int) {
  
  master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", masterAddr, masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}
	
  
  N := len(rlReply.ReplicaList)
  servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	rarray := make([]int, 1)
	karray := make([]int64, 1)
	put := make([]bool, 1)
		
	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	leader := 0

	reply := new(masterproto.GetLeaderReply)
	if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
		log.Fatalf("Error making the GetLeader RPC\n")
	}
	leader = reply.LeaderId
	log.Printf("The leader is replica %d\n", leader)
  
  r := rand.Intn(N)
  numReqsatOnce := *numOutstandingReqs;
	rarray[0] = r
  karray[0] = 42
  put[0] = true
  
  var id int32 = int32(clientId);
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, 0}, 0}

	reqReply := new(genericsmrproto.ProposeReplyTS)
  
  var timestamps map[int32]int64
  timestamps = make(map[int32]int64)

  clientBatchSize := 100

  for {
    for i := 0; i < numReqsatOnce; i++ { 
        args.CommandId = id
        args.Command.Op = state.PUT
        args.Command.K = state.Key(karray[0])
        args.Command.V = state.Value(0)
        writers[leader].WriteByte(genericsmrproto.PROPOSE)
        args.Marshal(writers[leader])

        start := time.Now() 
        start_nano := start.UnixNano()
        timestamps[id] = start_nano
        //fmt.Printf("Sending request with id %v\n", id)
        
        if (i % clientBatchSize == 0) {
          writers[leader].Flush()
        }
        id = id + int32(numClients);
    }
    
    writers[leader].Flush()

    for i := 0; i < numReqsatOnce; i++ {

	    rerr := false 

      if err := reqReply.Unmarshal(readers[leader]); err != nil {
			  fmt.Println("Error when reading:", err)
			  rerr = true
			  continue
		  }
		
      if rerr {
			  reply := new(masterproto.GetLeaderReply)
			  master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
			  leader = reply.LeaderId
			  log.Printf("New leader is replica %d\n", leader)
		  } else {
        end := time.Now()
        end_nano := end.UnixNano()
        idx := reqReply.CommandId
        fmt.Printf("#req%v %v %v %v\n", idx, timestamps[idx]/(1000.0*1000), end_nano/(1000.0*1000), clientId) 
      }
    }
  
    numReqsatOnce = clientBatchSize
  }

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
}
