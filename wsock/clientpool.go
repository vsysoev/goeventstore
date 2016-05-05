package wsock

//DONE:60 Incapsulate all client pool activity here
type (
	//ClientPool holds active clients pool to manage pub/sub
	ClientPool []*Client
)

// Append adds client to pool
func (c ClientPool) Append(cli *Client) ClientPool {
	return append(c, cli)
}

// Delete removes client from the pool
func (c ClientPool) Delete(cli *Client) ClientPool {
	for i, v := range c {
		if v == cli {
			return append(c[:i], c[i+1:]...)
		}
	}
	return c
}
