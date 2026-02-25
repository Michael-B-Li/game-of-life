// Centralised RPC stuff so no circular dependencies/duplicate code

package gol

import "uk.ac.bris.cs/gameoflife/util"

// WorkerInitRequest carries initial configuration for a distributed worker
type WorkerInitRequest struct {
	Slice           [][]byte
	Width           int
	TotalHeight     int
	StartRow        int
	WorkerIndex     int
	TotalWorkers    int
	TopNeighbour    string
	BottomNeighbour string
}

// WorkerInitResponse acknowledges worker initialisation
type WorkerInitResponse struct{}

// WorkerEvolveRequest triggers a single simulation turn on a worker
type WorkerEvolveRequest struct {
	Turn         int
	CollectFlips bool
}

// WorkerEvolveResponse contains the delta in alive cells and optional flip set
type WorkerEvolveResponse struct {
	Delta        int
	FlippedCells []util.Cell
}

// WorkerSliceRequest asks a worker for its current slice of the world
type WorkerSliceRequest struct{}

// WorkerSliceResponse returns the worker's slice
type WorkerSliceResponse struct {
	Slice [][]byte
}

// WorkerHaloDelivery is used when sending halo rows to neighbours
type WorkerHaloDelivery struct {
	Turn      int
	Direction string
	Row       []byte
}

// WorkerHaloAck acknowledges the receipt of a halo row
type WorkerHaloAck struct{}

// WorkerStopRequest instructs a worker to terminate
type WorkerStopRequest struct{}

// WorkerStopResponse acknowledges worker termination request
type WorkerStopResponse struct{}

// KillRequest/Response used to stop broker and workers
type KillRequest struct{}

type KillResponse struct{}
