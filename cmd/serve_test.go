package cmd

import "testing"

func TestServeHelp(t *testing.T) {
	if exitCode := testMain([]string{"cmd", "serve", "--help"}); exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}
}

func TestSubmitHelp(t *testing.T) {
	if exitCode := testMain([]string{"cmd", "submit", "--help"}); exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}
}

func TestServeRejectsBindWithoutOptIn(t *testing.T) {
	if exitCode := testMain([]string{"cmd", "serve", "--bind"}); exitCode != 1 {
		t.Fatalf("expected exit code 1 for --bind, got %d", exitCode)
	}
}

func TestServeRequiresTokenForNonLoopback(t *testing.T) {
	if exitCode := testMain([]string{"cmd", "serve", "--addr", "0.0.0.0:8080"}); exitCode != 1 {
		t.Fatalf("expected exit code 1 for a non loopback address without token, got %d", exitCode)
	}
}
