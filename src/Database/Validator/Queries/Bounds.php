<?php

namespace Utopia\Database\Validator\Queries;

use DateTime;

/**
 * The limits of an adapter that query values are checked against: the type of its document ids,
 * the length of a document uid and the range of a datetime.
 */
final readonly class Bounds
{
    public function __construct(
        public string $idAttributeType,
        public int $maxUIDLength,
        public DateTime $minDateTime,
        public DateTime $maxDateTime,
    ) {
    }
}
