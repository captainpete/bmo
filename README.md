# BMO

BMO takes JSON objects through standard input and uploads them to a specified table in the RethinkDB server.

Example usage:

`some_script | bmo -node localhost -database sophia -table bmo_test`
