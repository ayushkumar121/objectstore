build:
	go build -o bin/objectstore .

run:
	./bin/objectstore
	
clean:
	rm -rf bin