package pkg

import "google.golang.org/protobuf/compiler/protogen"

func GenerateClient(g *protogen.GeneratedFile, model *protogen.Message) error {
	g.P(
		`import (
      "context"
      kafka "github.com/segmentio/kafka-go" 
      "proto"
    )`,
	)

	objectName := model.GoIdent.GoName
	producerStructName := model.GoIdent.GoName + "Producer"
	consumerStructName := model.GoIdent.GoName + "Consumer"

	// Producer code
	g.P("type ", producerStructName, " struct {")
	g.P("  writer *kafka.Writer")
	g.P("}")
	g.P()

	g.P("func New", objectName, "Producer(writer *kafka.Writer) *", producerStructName, "{")
	g.P("  return &", producerStructName, "{writer: writer}")
	g.P("}")
	g.P()

	g.P("func (p *", producerStructName, ") Produce(ctx context.Context, msgs ...", objectName, ") error {")
	g.P("  kafkaMsgs := make([]kafka.Message, 0, len(msgs))")
	g.P("  for _, e := range msgs {")
	g.P("    res, err := proto.Marshal(e)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P()
	g.P("    kafkaMsgs = append(kafkaMsgs, kafka.Message{Value: res})")
	g.P("  }")

	g.P("  err := p.writer.WriteMessages(ctx, kafkaMsgs...)")
	g.P("  if err != nil {")
	g.P("    return err")
	g.P("  }")
	g.P()

	g.P("  return nil")
	g.P("}")
	g.P()

	// Consumer code
	g.P("type ", consumerStructName, " struct {")
	g.P("  reader *kafka.Reader")
	g.P("}")
	g.P()

	g.P("func New", consumerStructName, "(reader *kafka.Reader) *", consumerStructName, "{")
	g.P("  return &", consumerStructName, "{reader: reader}")
	g.P("}")
	g.P()

	g.P("func (c *", consumerStructName, ") Consume(ctx context.Context, handler ConsumerFunc[", objectName, "]) error {")
	g.P("  for {")
	g.P("    msg, err := c.reader.ReadMessage(ctx)")
	g.P("    if err != nil {")
	g.P("      return nil")
	g.P("    }")
	g.P()

	g.P("    entity := new(", objectName, ")")
	g.P("    err = proto.Unmarshal(msg.Value, entity)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P()

	g.P("    res, err := handler(ctx, entity)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P()

	g.P("    switch res {")
	g.P("    case ACK:")
	g.P("      err = c.reader.CommitMessage(ctx, msg)")
	g.P("      if err != nil {")
	g.P("        return err")
	g.P("      }")
	g.P("    default: //TODO: Add other types handling")
	g.P("      return nil")
	g.P("    }")
	g.P("  }")
	g.P("}")

	return nil
}
