<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class QueryErrorException extends NestboxException
{
    function __construct(string $message = "", int $code = 0, $previous = null, array $errorInfo = [])
    {
        $message = (!$message and $errorInfo) ? "MySQL error $errorInfo[1]: $errorInfo[2] ($errorInfo[0])" : $message;

        parent::__construct($message, $code, $previous);
    }
}
