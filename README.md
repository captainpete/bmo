# BMO

BMO takes a JSON stream from standard input and upload the objects to RethinkDB.
BMO uses parallel inserts and is written in [Golang](https://golang.org/).

Check out the [issues](https://github.com/cosmicturtle/bmo/issues), create a pull request, stay classy.

`some_script | bmo -node="localhost" -database="test" -table="bmo_ingress"`

<img src="https://raw.githubusercontent.com/cosmicturtle/bmo/master/bmo.png"/>
