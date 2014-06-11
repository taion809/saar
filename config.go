package main

import (
    "code.google.com/p/gcfg"
)

type Config struct {
    Database struct {
        Connection string
    }
}

func LoadConfig(filename string) (Config, error) {
    var c Config

    err := gcfg.ReadFileInto(&c, filename)

    return c, err
}
