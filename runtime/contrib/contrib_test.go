package contrib_test

import (
	"testing"

	"github.com/yuchanns/spacenet/runtime/contrib"
)

func TestResolveOrderDedupeAndCycle(t *testing.T) {
	a := contrib.New("a")
	b := contrib.New("b", contrib.WithDeps(a))
	c := contrib.New("c", contrib.WithDeps(b, a))

	got, err := contrib.Resolve([]*contrib.Contrib{c, a})
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(got) != 3 || got[0].Name() != "a" || got[1].Name() != "b" || got[2].Name() != "c" {
		t.Fatalf("unexpected order: %v", got)
	}

	if _, err := contrib.Resolve([]*contrib.Contrib{contrib.New("")}); err == nil {
		t.Fatal("expected error for empty contrib name")
	}

	x := contrib.New("x")
	y := contrib.New("y", contrib.WithDeps(x))
	contrib.WithDeps(y)(x)
	if _, err := contrib.Resolve([]*contrib.Contrib{x}); err == nil {
		t.Fatal("expected cycle error")
	}

	// Direct entries take precedence over transitive defaults of the same name.
	defA := contrib.New("shared")
	customA := contrib.New("shared")
	consumer := contrib.New("consumer", contrib.WithDeps(defA))
	got2, err := contrib.Resolve([]*contrib.Contrib{consumer, customA})
	if err != nil {
		t.Fatalf("Resolve precedence failed: %v", err)
	}
	if len(got2) != 2 || got2[0] != customA || got2[1] != consumer {
		t.Fatalf("expected direct customA to win over transitive default")
	}
}
