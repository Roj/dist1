SERVER?=ftpserver
ROOTDIR?=/

build:
	docker build . -t repo_client_1 
analyze:
	docker run --net=tp1_default repo_client_1 analyze $(SERVER)
summary:
	docker run --net=tp1_default repo_client_1 summary $(SERVER) $(ROOTDIR)

