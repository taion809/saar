package main

import (
    "database/sql"
    "github.com/coopernurse/gorp"
    "github.com/lib/pq"
    "math/rand"
    "time"
)

type DamConnection struct {
    Handle *gorp.DbMap
}

type Asset struct {
    AssetId        int64         `db:"asset_id"`
    MimeType       string        `db:"mime_type_name"`
    MimeTypeId     sql.NullInt64 `db:"mime_type_id"`
    IsUploaded     bool          `db:"is_uploaded"`
    IsReleased     bool          `db:"is_released"`
    UploadedBy     int           `db:"uploaded_by_user_id"`
    OwnedBy        int           `db:"owned_by_user_id"`
    ExpirationDate pq.NullTime   `db:"expiration_date"`
    Created        time.Time     `db:"created"`
    Updated        time.Time     `db:"updated"`
    Deleted        pq.NullTime   `db:"deleted"`
    Datapoints     []Datapoint
}

type Datapoint struct {
    Id            int            `db:"id"`
    AssetId       int            `db:"asset_id"`
    DatapointId   int            `db:"datapoint_id"`
    DatapointName string         `db:"datapoint_name"`
    ValidValueId  sql.NullInt64  `db:"valid_value_id"`
    Value         sql.NullString `db:"value"`
    Description   sql.NullString `db:"description"`
    Created       time.Time      `db:"created"`
    Updated       time.Time      `db:"updated"`
    Deleted       pq.NullTime    `db:"deleted"`
}

func NewDamConnection(conn_string string) (*DamConnection, error) {
    db, err := sql.Open("postgres", conn_string)

    if err != nil {
        return nil, err
    }

    dbmap := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}

    return &DamConnection{Handle: dbmap}, nil
}

func (c *DamConnection) FetchRandomAssetId() (int64, error) {
    count, err := c.Handle.SelectInt(`
        SELECT count(asset_id) FROM assets
        WHERE asset_id IN (SELECT asset_id FROM container_tags_assets WHERE container_tag_id = 61)`)

    if err != nil {
        return 0, err
    }

    r := rand.New(rand.NewSource(time.Now().UnixNano()))

    offset := r.Int63n(count)

    asset_id, err := c.Handle.SelectInt(`
        SELECT asset_id FROM assets
        WHERE asset_id IN (SELECT asset_id FROM container_tags_assets WHERE container_tag_id = 61)
        LIMIT 1 OFFSET $1`, offset)

    return asset_id, err
}

func (c *DamConnection) FetchAssetById(id int) (Asset, error) {
    var asset Asset
    err := c.Handle.SelectOne(&asset, `SELECT assets.asset_id, assets.mime_type_id, mime_types.type as mime_type_name,
        assets.is_released, assets.is_uploaded, assets.uploaded_by_user_id,
        assets.owned_by_user_id, created, updated, deleted, expiration_date
        FROM assets 
        LEFT JOIN mime_types 
        ON mime_types.mime_type_id = assets.mime_type_id
        WHERE assets.asset_id = $1`, id)

    if err != nil {
        return asset, err
    }

    datapoints, err := c.FetchAssetDatapointCollection(id)

    if err != nil {
        return asset, err
    }

    asset.Datapoints = datapoints

    return asset, nil
}

func (c *DamConnection) FetchAssetDatapointCollection(id int) ([]Datapoint, error) {
    var datapoints []Datapoint
    _, err := c.Handle.Select(&datapoints, `
        SELECT adv.id, adv.asset_id, adv.datapoint_id, datapoints.name as datapoint_name,
        adv.valid_value_id, adv.value, adv.description, adv.created, adv.updated, adv.deleted
        FROM assets_datapoints_values AS adv
        LEFT JOIN datapoints
        ON datapoints.datapoint_id = adv.datapoint_id
        WHERE asset_id = $1`, id)

    if err != nil {
        return nil, err
    }

    return datapoints, nil
}
