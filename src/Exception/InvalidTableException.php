<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox\Exception;

class InvalidTableException extends NestboxException
{
    function __construct(string $message = "", int $code = 0, $previous = null, string $table = "")
    {
        $message = (!$message and $table) ? "Invalid or unknown table `$table`" : $message;

        parent::__construct($message, $code, $previous);
    }
}
