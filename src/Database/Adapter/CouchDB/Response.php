<?php

namespace Utopia\Database\Adapter\CouchDB;

class Response
{
    public const OK = 200;
    public const CREATED = 201;
    public const ACCEPTED = 202;
    public const NOT_MODIFIED = 304;
    public const BAD_REQUEST = 400;
    public const UNAUTHORIZED = 401;
    public const FORBIDDEN = 403;
    public const NOT_FOUND = 404;
    public const CONFLICT = 409;
    public const PRECONDITION_FAILED = 412;
}