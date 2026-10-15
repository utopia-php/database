<?php

namespace Utopia\Database\Relationship;

use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;

/**
 * The page of each parent's related documents that a relationship query's limit, offset and cursor ask for.
 *
 * Related documents are read for every parent at once, so the page is cut from each parent's documents after they
 * are grouped, not by the read. A cursor is a related document's ID, and a parent that does not hold it has an
 * empty page.
 */
final readonly class Page
{
    private function __construct(
        public ?int $limit,
        public int $offset,
        public ?string $cursor,
        public bool $before,
    ) {
    }

    /**
     * Split the pagination queries from the rest, as the page they ask for, or null when they ask for none.
     *
     * @param  array<Query>  $queries
     * @return array{0: self|null, 1: list<Query>}
     */
    public static function from(array $queries): array
    {
        $limit = null;
        $offset = null;
        $cursor = null;
        $before = false;
        $paginated = false;
        $rest = [];

        foreach ($queries as $query) {
            $method = $query->getMethod();

            switch ($method) {
                case Method::Limit:
                    $limit ??= self::integer($query->getValue());
                    break;
                case Method::Offset:
                    $offset ??= self::integer($query->getValue());
                    break;
                case Method::CursorAfter:
                case Method::CursorBefore:
                    if ($cursor === null) {
                        $value = $query->getValue();
                        $cursor = $value instanceof Document ? $value->getId() : (\is_string($value) ? $value : null);
                        $before = $method === Method::CursorBefore;
                    }
                    break;
                default:
                    $rest[] = $query;

                    continue 2;
            }

            $paginated = true;
        }

        return [$paginated ? new self($limit, $offset ?? 0, $cursor, $before) : null, $rest];
    }

    /**
     * @param  array<Document>  $documents  One parent's related documents, in their order
     * @return list<Document>
     */
    public function slice(array $documents): array
    {
        $documents = \array_values($documents);

        if ($this->cursor === null) {
            return \array_slice($documents, $this->offset, $this->limit);
        }

        $position = null;
        foreach ($documents as $index => $document) {
            if ($document->getId() === $this->cursor) {
                $position = $index;
                break;
            }
        }

        if ($position === null) {
            return [];
        }

        if ($this->before) {
            $preceding = \array_reverse(\array_slice($documents, 0, $position));

            return \array_reverse(\array_slice($preceding, $this->offset, $this->limit));
        }

        return \array_slice($documents, $position + 1 + $this->offset, $this->limit);
    }

    private static function integer(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
