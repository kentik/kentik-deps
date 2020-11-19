An experiment with code-generation as a substitute for generics.

This is an implementation of reservoir sampling meant for reuse by kentik services.  It's initial use is in the cloudhelixflow chfproxy service, where we're sampling unparsed UDP flow messages, but the intention is that it can be used anywhere we need to do sampling of a stream by fixed intervals.

The code is written using the https://github.com/joeshaw/gengen/ code-generation tool.  In order to create an instantiation of this code for a new datatype, you'd need to:

- in the package where you want to instantiate, create a go source file with the go-generate tag

    ```
    //go:generate $GOPATH/bin/gengen -o . github.com/kentik/common/sampling/reservoir <data-type>
    ```

  where <data-type> is a visible go type you want to sample

- download and build the gengen package:
    ```
    go get github.com/joeshaw/gengen
    ```

- in the package where you want to instantiate, generate the code
    ```
    go generate
    ```

- (hack) remove the instantiated reservoir_test.go src file, which won't match your instantiated type.  gengen shouldn't really generate this file, but it does, at least for now.

In cloudhelixflow, all but the first steps above are automated through the "generate-reservoir" Makefile target; take a look at that for guidance.
