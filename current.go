package main
import (
//	"labix.org/v2/mgo"
)

type Datestamp int64

type ScalarValue struct {
	VarID int
	BoxID int
	Value float32
	TimePoint Datestamp
}

type VectorValue struct {
	VecID int
	BoxID int
	TimePoint Datestamp
}
type SystemState struct {
	scalar ScalarValue

}
var sysState SystemState

func init_system_state (){
	sysState.scalar.BoxID = 1
	sysState.scalar.Value = 1.0
	sysState.scalar.VarID = 1
	sysState.scalar.TimePoint = 1
}
func main () {
	init_system_state()
}