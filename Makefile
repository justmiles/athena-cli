build:
	goreleaser release --snapshot --rm-dist

publish:
	goreleaser release --rm-dist