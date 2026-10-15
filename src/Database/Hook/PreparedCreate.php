<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Document;

/**
 * One create whose new related documents the relationship hook prepares before writing them.
 */
final class PreparedCreate
{
    /**
     * @param  bool  $deferred  Whether the prepared documents wait to be written together, in a savepoint that can
     *                          take the attempt back; otherwise each is written where it would be written on its own
     */
    public function __construct(
        public readonly bool $deferred,
    ) {
    }

    /**
     * @var list<array{Document, Document}> The prepared documents not written yet, each after its collection, in
     *                                      the order they are to be written
     */
    public array $documents = [];

    /**
     * @var array<string, array<string, true>> The documents not finished preparing, which one by one would not be
     *                                         written yet, by collection id and document id
     */
    public array $preparing = [];

    /**
     * @var array<string, Document> The collections read while preparing, by id
     */
    public array $collections = [];
}
