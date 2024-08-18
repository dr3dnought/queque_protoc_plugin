package pkg

import "google.golang.org/protobuf/compiler/protogen"

func GenerateUtils(g *protogen.GeneratedFile) {
	g.P(
		`import (
      "context"
    )`,
	)
	g.P()

	g.P("type Result uint8")
	g.P("const (")
	g.P("  ACK   Result = 0")
	g.P("  NACK  Result = 1")
	g.P("  DEFER Result = 2")
	g.P(")")
	g.P()

	g.P("type ConsumerFunc[T any] func(ctx context.Context, msg T) (Result, error)")
	g.P()
}
