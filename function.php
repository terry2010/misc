<?php
/**
 * 老代码翻出来的一些函数,也不知道什么时候写的
 */

function array_slice_assoc($array, $offset, $length = null, $preserve_keys = FALSE) {
    $key = array_slice(array_keys($array), $offset, $length, $preserve_keys);
    $value = array_slice(array_values($array), $offset, $length, $preserve_keys);


    return array_combine($key, $value);
}
