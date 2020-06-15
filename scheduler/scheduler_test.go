package scheduler

import "testing"
import "fmt"

func Test_getPrimaryDomain(t *testing.T) {
	host := "www.sohou.com"
	ret, err := getPrimaryDomain(host)
	if err != nil {
		t.Errorf("test error: %s", err.Error())
	}
	fmt.Println(ret)
}
