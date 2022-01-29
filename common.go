package partitioner

// Handler high order function to be executed
// nit err for completed, otherwise infinite retry
type Handler func() error
