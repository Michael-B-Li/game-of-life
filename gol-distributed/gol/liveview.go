package gol

import (
	"net"
	"net/rpc"
	"sync"

	"uk.ac.bris.cs/gameoflife/util"
)

/* SERVER USED BY CONTROLLER FOR LIVE VIEW */

// LiveViewerUpdate is sent by the broker once every completed turn
// We forward only the cells that flipped to avoid sending an entire world state over RPC
type LiveViewerUpdate struct {
	Turn         int
	FlippedCells []util.Cell
}

// LiveViewerAck acknowledges  live viewer update
type LiveViewerAck struct{}

// liveViewServer hosts a RPC endpoint inside the controller so the broker can send turn
// updates as ProcessTurns only returns when all turns are complete
type liveViewServer struct {
	events    chan<- Event  // sends events to the main event channel
	server    *rpc.Server   // RPC server bound to localhost
	listener  net.Listener  // track listener for shutdown
	done      chan struct{} // closed to stop accepting work
	closeOnce sync.Once
}

func startLiveViewServer(events chan<- Event) (*liveViewServer, string, error) {
	handler := &liveViewServer{
		events: events,
		done:   make(chan struct{}),
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("LiveView", handler); err != nil {
		return nil, "", err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	handler.server = srv
	handler.listener = listener

	go srv.Accept(listener)

	return handler, listener.Addr().String(), nil
}

// Close stops connections and  prevents further events from being emitted,
// ensuring SDL shuts down cleanly even if the broker keeps running
func (l *liveViewServer) Close() {
	if l == nil {
		return
	}
	l.closeOnce.Do(func() {
		close(l.done)
		if l.listener != nil {
			_ = l.listener.Close()
		}
	})
}

// Update, the RPC endpoint, is called remotely by the broker for every completed turn
// Translates the flipped cell list into the existing event stream for the SDL
func (l *liveViewServer) Update(req LiveViewerUpdate, _ *LiveViewerAck) error {
	select {
	case <-l.done:
		return nil
	default:
	}

	if len(req.FlippedCells) > 0 {
		l.events <- CellsFlipped{
			CompletedTurns: req.Turn,
			Cells:          req.FlippedCells,
		}
	}
	l.events <- TurnComplete{CompletedTurns: req.Turn}
	return nil
}

// shutdownLiveServer safely shuts down the live view server
func shutdownLiveServer(server *liveViewServer) {
	if server != nil {
		server.Close()
	}
}
