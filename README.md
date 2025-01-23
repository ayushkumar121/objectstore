# Objectstore

Blog storage server

## Design

/*
Following http methods needs to be implemented
PUT /bucket/foo/bar.txt
Authorization: ????

GET /bucket/foo/bar.txt
Authorization: ????

GET /bucket/foo <--List files
Authorization: ????

DELETE /bucket/foo/bar.txt
Authorization: ????

bucket
	-> Files
		-> Data
		-> Metadata
			Name: ...
Access List
	Ayush -> 
		/bucket/foo/bar.txt -> r/rw
		/bucket/foo/ -> r/rw
		/bucket/ ->
*/
