# Build SQL Efficiency Checker yourself

## Compile the Go binary

This section outlines steps to install this `sql_efficiency_check` app on Mac.

1. Download and install [GO](https://go.dev/doc/install)
2. From the command line in the `sql_efficiency_check` directory, run the go build command (`$ go build`) to compile the code into an executable.
- You've compiled the application into an executable so you can run it. But to run it currently, your prompt needs either to be in the executable's directory, or to specify the executable's path.
- Next, you'll install the executable so you can run it without specifying its path.
3. Run `$ go install` to move the binary to app directory.
4. Discover the Go install path, where the go command will install the current package.
- You can discover the install path by running the go list command, as in the following example: `$ go list -f '{{.Target}}'`
- For example, the command's output might say `/Users/bryankwon/go/bin/sql_efficiency_check`, meaning that binaries are installed to `/Users/bryankwon/go/bin`. You'll need this install directory in the next step.
5. Add the Go install directory to your system's shell path. That way, you'll be able to run your program's executable without specifying where the executable is.
- EXAMPLE: `$ export PATH=$PATH:/Users/bryankwon/go/bin`
6. Run your application by simply typing its name. To make this interesting, open a new command prompt and run the hello executable name in some other directory.

## Dockerfile self build

### Build Docker image

```bash
git clone git@github.com:cockroachlabs/sql_efficiency_check.git
cd sql_efficiency_check
env GOOS=linux GOARCH=amd64 go build ./sql_efficiency_check
docker build -t sql_efficiency_check -f ./Dockerfile.scratch .
```

### Run Docker image

```bash
docker run  -it sql_efficiency_check /sql_efficiency_check -conn postgresql://root@192.168.0.100:26257/defaultdb?sslmode=disable -maxStmt 2
```