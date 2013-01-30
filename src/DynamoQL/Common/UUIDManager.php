<?php
namespace DynamoQL\Common;
/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 7:17 PM
 */
class UUIDManager
{
    // 32 bit: 4294967295
    // 64 bit: 18446744073709551615
    //           379811802031484680
    // format: <hash[len-(bit_count/8)]><time[(bit_count/16)]><rand[(bit_count/16)]>
    public static function generate( $data, $bit_count = 32 )
    {
        $hex_char_count = $bit_count / 4;
        $exw            = round($hex_char_count / 8); // random width
        $data           = sha1($data);
        $data           = substr($data, 0, $hex_char_count - $exw); // hex
        $random_part    = substr(sha1(microtime(true) * 1000000 + rand() * 10000000), -$exw); // dec
        $final          = $data . $random_part;
        $final          = base_convert($final, 16, 10);

        //return $data."-".$random_part."(".$final.")";

        return $final;
    }

    public static function extract( $data )
    {
        $data           = base_convert($data, 10, 16);
        $bit_count      = strlen($data) * 4;
        $hex_char_count = $bit_count / 4;
        $exw            = round($hex_char_count / 8);

        return substr($data, 0, -$exw);
    }

    public static function compare( $uuid1, $uuid2 )
    {

        return UUIDManager::extract($uuid1) == UUIDManager::extract($uuid2);
    }
}
