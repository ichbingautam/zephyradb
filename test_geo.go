package main

import "fmt"

func main() {
    lat := 22.543764
    lon := 113.936698
    latMin := -85.05112878
    latMax := 85.05112878
    lonMin := -180.0
    lonMax := 180.0
    step := 26
    
    latOffset := (lat - latMin) / (latMax - latMin)
    lonOffset := (lon - lonMin) / (lonMax - lonMin)
    
    fmt.Printf("lat_offset: %f\n", latOffset)
    fmt.Printf("lon_offset: %f\n", lonOffset)
    
    latOffset *= float64(uint64(1) << step)
    lonOffset *= float64(uint64(1) << step)
    
    fmt.Printf("lat_offset after step: %f -> %d\n", latOffset, uint32(latOffset))
    fmt.Printf("lon_offset after step: %f -> %d\n", lonOffset, uint32(lonOffset))
}
