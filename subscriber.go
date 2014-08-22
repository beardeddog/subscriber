package main

import (
	"errors"
	"fmt"
	"github.com/gmallard/stompngo"
	flag "github.com/ogier/pflag"
	"net"
	"os"
	"strings"
)

var (
	nameString string = "subscriber"
	verString  string = "1.0.0"

	help     *bool   = flag.BoolP("help", "h", false, "show help")
	queue    *string = flag.StringP("queue", "q", "", "queue, prepend with /topic/ for topic")
	broker   *string = flag.StringP("broker", "b", "", "the broker to use, fqhn:port")
	verbose  *bool   = flag.BoolP("verbose", "v", false, "turn on verbose logging")
	version  *bool   = flag.Bool("version", false, "show version")
	username *string = flag.String("username", "", "username override")
	password *string = flag.String("password", "", "password override")
)

// main
func main() {
	// parse command line
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *version {
		fmt.Println(Version())
		os.Exit(0)
	}

	// need a broker and a queue
	if *broker == "" || *queue == "" {
		fmt.Println("Need -t and -q")
		flag.Usage()
		os.Exit(1)
	}

	var conn Subscriber
	conn.SetMsgHeader(*queue)

	// get message off test-collection
	err := conn.Connect("tcp", "STOMP", *broker)
	if err != nil {
		fmt.Printf("ERROR: Connect returned error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Connect succeeded\n")
	count := 0
	for {
		fmt.Printf("Getting messages from: %+v\n", *queue)
		msg, hdrs, err := conn.Get()
		count++
		if err != nil {
			fmt.Printf("ERROR: Get returned error: %+v\n", err)
			os.Exit(1)
		} else {
			fmt.Printf("Get succeeded on %+v\n", conn.GetActiveSubscriber())
			fmt.Printf("---msg id: \"%+v\"\n", hdrs["message-id"])
			fmt.Printf("--headers: \"%+v\"\n", hdrs)
			fmt.Printf("--message: \"%+v\"\n", msg)
			fmt.Printf("----count: %+v\n---------------------------------\n", count)
		}
	}
	err = conn.Disconnect()
	if err != nil {
		fmt.Printf("ERROR: Disconnect returned error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Disconnect succeeded\n")
}

// Version returns the package's current version, useful by apps using this package
func Version() (versions string) {
	return nameString + " " + verString
}

type Subscriber struct {
	transportConn     net.Conn
	transportProtocol string //tcp or ssl or ...
	messageProtocol   string // STOMP or ...
	stompConnHeader   stompngo.Headers
	// sub
	stompConn         *stompngo.Connection
	stompMsgHeader    stompngo.Headers
	stompMsgHeaderSet bool
	stompReceiveChan  <-chan stompngo.MessageData
	connected         bool   // true if connected to a collector
	stompMsgUuid      string // used when subscribing/receiving messages from a queue
	// cfgsrv
	collectorIndex int    // index of the collector being used
	MBSTarget      string // full broker path
}

// isConnected return true if connected to a message bus broker, false otherwise.
//
//   if sub.isConnected()
func (s *Subscriber) isConnected() bool {
	return s.connected
}

// Connect makes a transport protocol (tcp) connection, then a message protocol (STOMP) connection to the
// message bus (activeMQ) on the broker, returning nil if successful, else an error.
//
//   err = sub.Connect("tcp", "STOMP", "host.com:3333")
func (s *Subscriber) Connect(tpProtocol, msgProtocol, collectorport string) (e error) {
	s.transportProtocol = tpProtocol
	s.messageProtocol = msgProtocol
	s.MBSTarget = collectorport

	fmt.Println("INFO: Connecting ... ")
	fmt.Println("INFO: " + s.transportProtocol + " dialing ... " + collectorport)
	switch s.transportProtocol {
	case "tcp":
		s.transportConn, e = net.Dial(s.transportProtocol, collectorport)
		// defer s.transportConn.Close()
		if e != nil {
			fmt.Println("INFO: failed to connect to " + collectorport + " " + fmt.Sprintf("%v", e))
			// continue
		} else {
			fmt.Println("INFO: ... connected to " + collectorport)
			// do the STOMP connection here
			switch strings.ToLower(s.messageProtocol) {
			case "stomp":
				// host, _ := net.LookupAddr(strings.Split(s.transportConn.RemoteAddr().String(), ":")[0])
				s.stompConnHeader = stompngo.Headers{
					"host", collectorport,
					"accept-version", "1.1"}
				if *username != "" {
					s.stompConnHeader = append(s.stompConnHeader, "login", *username)
					s.stompConnHeader = append(s.stompConnHeader, "passcode", *password)
				}
				fmt.Printf("INFO Connect: %v\n", s.stompConnHeader)
				s.stompConn, e = stompngo.Connect(s.transportConn, s.stompConnHeader)
				if e != nil {
					return errors.New("ERROR: Connect: failed to connect [" + s.transportProtocol + "][" + s.messageProtocol + "] on host: " + collectorport)
				}
				if !s.stompMsgHeaderSet {
					return errors.New("ERROR: Connect: subscribe attempt to broker with header not set.")
				} else {
					// connect to the receive channel
					s.stompReceiveChan, e = s.stompConn.Subscribe(s.stompMsgHeader)
					if e != nil {
						return errors.New("ERROR: Connect: stompngo.Connect attempt to broker failed with: " + fmt.Sprintf("%v", e))
					}
				}
			case "ampq":
				fallthrough
			case "openwire":
				fallthrough
			default:
				return errors.New("ERROR: Connect: message protocal [" + s.messageProtocol + "]: not supported")
			}
			// on successful connection
			s.connected = true
			return nil
		}
	case "ssl":
		fallthrough
	default:
		return errors.New("ERROR: Connect: transport protocal [" + s.transportProtocol + "]: not supported")
	}
	return nil
}

// Disconnect closes the messageProtocal connection then the transportProtocol connection
//
//   error := sub.Disconnect
func (s *Subscriber) Disconnect() (e error) {
	if s.isConnected() {
		switch strings.ToLower(s.messageProtocol) {
		case "stomp":
			e = s.stompConn.Unsubscribe(s.stompMsgHeader)
			if e != nil {
				return errors.New("ERROR: Disconnect: unsubscribe [" + s.messageProtocol + "] failed with " + fmt.Sprintf("%v", e))
			}
			e = s.stompConn.Disconnect(s.stompConnHeader)
			if e != nil {
				return errors.New("ERROR: Disconnect: disconnect [" + s.messageProtocol + "] failed with " + fmt.Sprintf("%v", e))
			}
			e = s.transportConn.Close()
			if e != nil {
				return errors.New("ERROR: Disconnect: close [" + s.transportProtocol + "] failed with " + fmt.Sprintf("%v", e))
			}
		case "ssl":
			fallthrough
		default:

		}
		s.connected = false
		return nil
	}
	return errors.New("ERROR: Disconnect: disconnect attempt when not connected")
}

// SetMsgHeader sets up the STOMP message header for subscribing, id is optional
// if not specified a unique one will be created using StompnGo's uuid function
//
//   sub.SetMsgHeader("MARS2")
//   sub.SetMsgHeader("MARS2", "myid")
func (s *Subscriber) SetMsgHeader(from string, id ...string) {
	s.stompMsgHeader = append(s.stompMsgHeader, "destination", from)
	if len(id) == 0 {
		id = append(id, stompngo.Uuid()) // this creates a unique id
	}
	s.stompMsgHeader = append(s.stompMsgHeader, "id", id[0])
	s.stompMsgHeaderSet = true
}

// Get retrieves the next message off the queue and returns the body in message as a string,
// the header as a map and an error of nil, else an error if something went wrong
//
//   message, headers, error := sub.Get()
func (s *Subscriber) Get() (message string, header map[string]string, e error) {
SUBTOP:
	if s.isConnected() {
		header = map[string]string{}
		m := <-s.stompReceiveChan
		if m.Error != nil {
			err := fmt.Sprintf("%v", m.Error)
			// fmt.Println("INFO: error: " + err)
			if err == "EOF" {
				// Get failed, attempt to reconnect
				fmt.Println("WARN: failed to get message, send error: " + fmt.Sprintf("%v", m.Error))
				e = s.Disconnect()
				s.connected = false
				// time.Sleep(20 * time.Second)
				fmt.Println("INFO: Attempting reconnect ...")
				e = s.Connect(s.transportProtocol, s.messageProtocol, s.MBSTarget)

				// // resend the previous message
				// fmt.Println("INFO: resending: " + s.messageCache[1]) // here comment out
				// e = s.stompPubConn.Send(s.stompPubMsgHeader, s.messageCache[1])
				goto SUBTOP
			} else {
				return "", header, errors.New("ERROR: Get: retrieved message with error: " + fmt.Sprintf("%v", m.Error))
			}
		}
		h := m.Message.Headers
		for j := 0; j < len(h)-1; j += 2 {
			header[h[j]] = h[j+1]
		}
		message = string(m.Message.Body)
		return message, header, nil
	}
	return "", header, errors.New("ERROR: Get: get attempt to broker when not connected.")
}

// GetActiveSubscriber returns the collector:port being subscriber from
func (s *Subscriber) GetActiveSubscriber() string {
	return s.MBSTarget
}
