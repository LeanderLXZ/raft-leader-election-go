all: TestInitialElection TestPreviousLeaderRejoin TestReElectionFail TestReElectionSuccess TestReElectionSuccess2

TestInitialElection:
	cd src/raft; \
	export HOME="${CURDIR}"; \
	go test -run TestInitialElection -v -timeout=120s 

TestPreviousLeaderRejoin:
	cd src/raft; \
	export HOME="${CURDIR}"; \
	go test -run TestPreviousLeaderRejoin -v -timeout=120s

TestReElectionFail:
	cd src/raft; \
	export HOME="${CURDIR}"; \
	go test -run TestReElectionFail -v -timeout=120s

TestReElectionSuccess:
	cd src/raft; \
	export HOME="${CURDIR}"; \
	go test -run TestReElectionSuccess1 -v -timeout=120s

TestReElectionSuccess2:
	cd src/raft; \
	export HOME="${CURDIR}"; \
	go test -run TestReElectionSuccess2 -v -timeout=120s

