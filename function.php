<?php

function array_slice_assoc($array, $offset, $length = null, $preserve_keys = FALSE) {
    $key = array_slice(array_keys($array), $offset, $length, $preserve_keys);
    $value = array_slice(array_values($array), $offset, $length, $preserve_keys);


    return array_combine($key, $value);
}
