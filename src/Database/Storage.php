<?php

namespace Utopia\Database;

final readonly class Storage
{
    public const string UID = '_uid';

    public const string SEQUENCE = '_id';

    public const string COLLECTION = '_collection';

    public const string TENANT = '_tenant';

    public const string CREATED_AT = '_createdAt';

    public const string UPDATED_AT = '_updatedAt';

    public const string PERMISSIONS = '_permissions';

    public const string DISTANCE = '_distance';

    public const string DELETED_AT = '_deletedAt';

    public const string PERMS_SUFFIX = '_perms';

    public const string PERM_DOCUMENT = '_document';

    public const string PERM_TYPE = '_type';

    public const string PERM_PERMISSION = '_permission';

    public const string INDEX_PRIMARY = 'primary';

    public const string INDEX_CREATED_AT = '_created_at';

    public const string INDEX_UPDATED_AT = '_updated_at';

    public const string INDEX_TENANT_ID = '_tenant_id';

    public const string INDEX_1 = '_index1';

    public const string INDEX_PERMISSIONS_ID = '_permissions_id';

    public const string JOIN_ALIAS_PREFIX = 'j';

    /**
     * @var array<string, string>
     */
    private const array ATTRIBUTE_MAP = [
        Document::ID => self::UID,
        Document::SEQUENCE => self::SEQUENCE,
        Document::COLLECTION => self::COLLECTION,
        Document::TENANT => self::TENANT,
        Document::CREATED_AT => self::CREATED_AT,
        Document::UPDATED_AT => self::UPDATED_AT,
        Document::DELETED_AT => self::DELETED_AT,
        Document::PERMISSIONS => self::PERMISSIONS,
        Document::DISTANCE => self::DISTANCE,
    ];

    private function __construct()
    {
    }

    public static function column(string $attribute): string
    {
        return self::ATTRIBUTE_MAP[$attribute] ?? $attribute;
    }

    public static function attribute(string $column): string
    {
        return self::columnMap()[$column] ?? $column;
    }

    /**
     * @return array<string, string>
     */
    public static function attributeMap(): array
    {
        return self::ATTRIBUTE_MAP;
    }

    /**
     * @return array<string, string>
     */
    public static function columnMap(): array
    {
        return \array_flip(self::ATTRIBUTE_MAP);
    }

    public static function permissionsTable(string $collection): string
    {
        return $collection.self::PERMS_SUFFIX;
    }

    /**
     * The alias a join that declares none has its values returned under: the prefix and the join's
     * position among the query's joins, or the next number no other alias of the query takes.
     *
     * @param  array<string, true>  $taken  Lower-cased aliases in use; the alias returned is added
     */
    public static function joinAlias(int $position, array &$taken): string
    {
        do {
            $alias = self::JOIN_ALIAS_PREFIX.$position++;
        } while (isset($taken[$alias]));

        $taken[$alias] = true;

        return $alias;
    }
}
