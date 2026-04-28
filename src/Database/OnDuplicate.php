<?php

namespace Utopia\Database;

/**
 * Behavior when a create-style operation encounters an existing resource
 * (document, collection, attribute, index) with the same identifier.
 */
enum OnDuplicate: string
{
    /** Throw DuplicateException. Default — no tolerance. */
    case Fail = 'fail';

    /** Silently no-op when the resource already exists. */
    case Skip = 'skip';

    /** Overwrite / update the existing resource to match the incoming one. */
    case Upsert = 'upsert';

    /**
     * @return array<string>
     */
    public static function values(): array
    {
        return \array_map(fn (self $case) => $case->value, self::cases());
    }
}
