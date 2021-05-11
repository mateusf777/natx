package common

type GetStoreRequest struct {
	Key string
}

type GetStoreResponse struct {
	Value string
	Err   *string
}

type AddStoreRequest struct {
	Key   string
	Value string
}

type AddStoreResponse struct {
	Err *string
}
