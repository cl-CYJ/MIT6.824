package main

import (
    "encoding/json"
    "fmt"
    "bytes"
    "strings"
)

type Person struct {
    Name string 
    Age int 
}

func main()  {
    // 1. 使用 json.Marshal 编码
    person1 := Person{"张三", 24}
    bytes1, err := json.Marshal(&person1)
    if err == nil {
        // 返回的是字节数组 []byte
        fmt.Println("json.Marshal 编码结果: ", string(bytes1))
    }

    // 2. 使用 json.Unmarshal 解码
    str := `{"name":"李四","age":25}`
    // json.Unmarshal 需要字节数组参数, 需要把字符串转为 []byte 类型
    bytes2 := []byte(str) // 字符串转换为字节数组
    var person2 Person    // 用来接收解码后的结果
    if json.Unmarshal(bytes2, &person2) == nil {
        fmt.Println("json.Unmarshal 解码结果: ", person2.Name, person2.Age)
    }

    // 3. 使用 json.NewEncoder 编码
    person3 := Person{"王五", 30}
    // 编码结果暂存到 buffer
    bytes3 := new(bytes.Buffer)
    _ = json.NewEncoder(bytes3).Encode(person3)
    if err == nil {
        fmt.Print("json.NewEncoder 编码结果: ", string(bytes3.Bytes()))
    }

    // 4. 使用 json.NewDecoder 解码
    str4 := `{"name":"赵六","age":28}`
    var person4 Person
    // 创建一个 string reader 作为参数
    err = json.NewDecoder(strings.NewReader(str4)).Decode(&person4)
    if err == nil {
        fmt.Println("json.NewDecoder 解码结果: ", person4.Name, person4.Age)
    }
}
