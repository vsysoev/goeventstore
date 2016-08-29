package main

import (
	"strconv"
	"testing"
	"time"
)

func TestNewRPCParameterInterface(t *testing.T) {
	p := NewRPCParameterInterface(nil)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
}

func TestRPCParamStringAsString(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := "Hello world!"
	prms["paramter"] = expected
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsString("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamIntAsString(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := "100"
	prms["paramter"] = 100
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsString("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamFloatAsString(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := "100.5"
	prms["paramter"] = 100.5
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsString("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}

func TestRPCParamTimestampAsString(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	tNow := time.Now()
	expected := tNow.String()
	prms["paramter"] = tNow
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsString("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamStringAsTimestamp(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := time.Now()
	prms["paramter"] = expected.Format(time.RFC3339Nano)
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsTimestamp("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Equal(expected) {
		t.Fatal("Expected:", expected, ". Got:", s, ".")
	}
}
func TestRPCParamIntAsTimestamp(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := time.Now().Unix()
	prms["paramter"] = expected
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsTimestamp("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s.Unix() != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamFloatAsTimestamp(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := float64(time.Now().Unix())
	prms["paramter"] = expected
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsTimestamp("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if float64(s.Unix()) != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}

func TestRPCParamTimestampAsTimestamp(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	tNow := time.Now()
	expected := tNow
	prms["paramter"] = tNow
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsTimestamp("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamStringAsInt(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := int64(100)
	prms["paramter"] = strconv.FormatInt(expected, 10)
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsInt64("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamIntAsInt(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := int64(100)
	prms["paramter"] = expected
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsInt64("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}
func TestRPCParamFloatAsInt(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	expected := 100.5
	prms["paramter"] = expected
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsInt64("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != int64(expected) {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}

func TestRPCParamTimestampAsInt(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	tNow := time.Now()
	expected := tNow.Unix()
	prms["paramter"] = tNow
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsInt64("paramter")
	if err != nil {
		t.Fatal(err)
	}
	if s != expected {
		t.Fatal("Expected ", expected, ". Got ", s)
	}
}

func TestRPCParamStringAsMapString(t *testing.T) {
	prms := make(map[string]interface{}, 1)
	filter := make(map[string]interface{}, 1)
	filter["int"] = 1
	filter["string"] = "Hello world!!!"
	expected := filter
	prms["parameter"] = filter
	p := NewRPCParameterInterface(prms)
	if p == nil {
		t.Fatal("RPCParamterInterface should not be nil")
	}
	s, err := p.AsMapString("parameter")
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range s {
		if expected[k] != v {
			t.Fatal("Expected ", expected[k], ". Got ", v)
		}
	}
}
