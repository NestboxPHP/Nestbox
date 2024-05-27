<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class InvalidParameterValueType extends NestboxException
{
    function __construct(string $message = "", int $code = 0, \Exception $previous = null, string $variable = "", mixed $value = null)
    {
        $type = gettype($value);
        $value = match ($type) {
            "boolean", "integer", "double", "string" => $value,
            "NULL" => "NULL",
            default => serialize($value)
        };
        parent::__construct(trim("$message $variable ($type): $value"), $code, $previous);
    }
}
