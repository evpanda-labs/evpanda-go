// Package evpanda provides passive OCPI/OCPP traffic capture for embedding
// in OCPI servers and OCPP CSMS. It records protocol messages, buffers them
// in-process, and ships them in batches to the EVPanda ingestion API.
//
// The SDK stays out of the host's way: capture calls are non-blocking and
// never panic, memory is bounded, and under stress or network failure it
// drops data rather than degrading the application.
//
// The protocol is the client: [StartOCPI] returns an [OCPIClient],
// [StartOCPP] an [OCPPClient] — pick the one your service speaks.
//
//	// APIKey omitted ⇒ read from the EVPANDA_API_KEY env var.
//	panda, err := evpanda.StartOCPI(evpanda.OCPIConfig{
//		BaseConfig: evpanda.BaseConfig{Endpoint: "https://ingest.evpanda.io"},
//	})
//	if err != nil {
//		log.Printf("evpanda: %v (running inert)", err)
//	}
//	defer panda.Close()
//
//	panda.CaptureInbound(evpanda.OCPIMessageInput{ /* identity + HTTP */ })
//
// For OCPP, prefer the session handle returned by [OCPPClient.Connection]:
// it mints the connection ID and carries the identity, so per-frame calls
// carry neither.
package evpanda
