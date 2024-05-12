<?php

declare( strict_types = 1 );

namespace NestboxPHP\Nestbox\Exception;

use RuntimeException;

class NestboxException extends RuntimeException
{
    function __construct(string $message = "", int $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}

