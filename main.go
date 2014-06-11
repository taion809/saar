package main

import (
    "fmt"
)

func main() {
    config, err := LoadConfig("config.gcfg")

    fmt.Println(config.Database.Connection)

    if err != nil {
        panic(err)
    }

    db_conn, err := NewDamConnection(config.Database.Connection)

    if err != nil {
        panic(err)
    }

    rand_asset, err := db_conn.FetchRandomAssetId()
    if err != nil {
        panic(err)
    }

    fmt.Println("Random Asset: ", rand_asset)

    v, err := db_conn.FetchAssetById(9006621)

    if err != nil {
        panic(err)
    }

    // router := mux.NewRouter()
    // router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    fmt.Printf("Asset: %v \n", v)

    for _, val := range v.Datapoints {
        if val.DatapointName == "filename" {
            fmt.Println("The Filename is: ", val.Value.String)
            continue
        }

        // fmt.Printf("Key %d --> Value: %v \n", key, val)
    }
    // })

    // http.Handle("/", router)
    // http.ListenAndServe(":8080", nil)
}
