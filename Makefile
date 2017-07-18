SHELL := /bin/bash
REBAR = ./rebar3

all: compile

me:
	$(REBAR) compile skip_deps=true

shell:
	$(REBAR) shell


test:
	$(REBAR) do eunit, cover

eunit:
	$(REBAR) eunit

empty:
	rm -rf _build
	rm -rf rebar.lock

doc:
	apidoc -v -i doc/ -o doc/api/ -f .erl


dialyzer:
	./dialyzer.sh


clean:
	$(REBAR) clean

clean-doc:
	rm -rf doc/api

release:
	$(REBAR) do eunit, clean, release
	$(REBAR) tar


fresh: clean compile test