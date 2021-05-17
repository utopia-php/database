# Contributing

We would ❤️ for you to contribute and help make this library better! All contributions are welcome, especially new adapters (more below).

## Code of Conduct

Help us keep the community open and inclusive. Please read and follow our [Code of Conduct](https://github.com/appwrite/appwrite/blob/master/CODE_OF_CONDUCT.md).

## Submit a Pull Request 🚀

Branch naming convention is as following 

`TYPE-ISSUE_ID-DESCRIPTION`

example:
```
doc-548-submit-a-pull-request-section-to-contribution-guide
```

When `TYPE` can be:

- **feat** - is a new feature
- **doc** - documentation only changes
- **cicd** - changes related to CI/CD system
- **fix** - a bug fix
- **refactor** - code change that neither fixes a bug nor adds a feature

**All PRs must include a commit message with the changes description!** 

For the initial start, fork the project and use the `git clone` command to download the repository to your computer. A standard procedure for working on an issue would be to:

1. `git pull`, before creating a new branch, pull the changes from upstream. Your main needs to be up to date.
```
$ git pull
```
2. Create new branch from `main` like: `doc-548-submit-a-pull-request-section-to-contribution-guide`<br/>
```
$ git checkout -b [name_of_your_new_branch]
```
3. Work - commit - repeat ( be sure to be in your branch )

4. Push changes to GitHub 
```
$ git push origin [name_of_your_new_branch]
```

5. Submit your changes for review. If you go to your repository on GitHub, you'll see a `Compare & pull request` button. Click on that button.
6. Start a Pull Request
Now submit the pull request and click on `Create pull request`.
7. Get a code review approval/reject
8. After approval, merge your PR
9. GitHub will automatically delete the branch after the merge is done. (they can still be restored).

## Creating A New Adapter

To get started with implementing a new adapter, start by reviewing the [specification](/SPEC.md) to understand the goals of this library. The specification defines the NoSQL-inspired API methods and contains all of the functions a new adapter must support, including types, queries, paging, indexes, and especially emojis ❤️.. The capabilities of each adapter are defined in the `getSupportFor*` and `get*Limit` methods.

### File Structure

Below are outlined the most useful files for adding a new database adapter: 

```bash
.
├── src # Source code
│   └── Database
│       ├── Adapter/ # Where your new adapter goes!
│       ├── Adapter.php # Parent class for individual adapters
│       ├── Database.php # Database class - calls individual adapter methods
│       ├── Document.php # Document class - 
│       └── Query.php # Query class - holds query attributes, methods, and values
└── tests
    └── Database
        ├── Adapter/ # Extended from Base 
        └── Base.php # Parent class that holds all tests
```


### Extend the Adapter

Create your `NewDB.php` file in `src/Database/Adapter/` and extend the parent class:

```php
<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class NewDB extends Adapter
{
```

Only include dependencies strictly necessary for the database, preferably official PHP libraries, if available.

### Testing with Docker 

The existing test suite is helpful when developing a new database adapter. To get started with testing, add a stanza to `docker-compose.yml` with your new database, using existing adapters as examples. Use official Docker images from trusted sources, and provide the necessary `environment` variables for startup. Then, create a new file for your NewDB in `tests/Database/Adapter`, extending the `Base.php` test class. The specific `docker-compose` command for testing can be found in the [README](/README.md#tests).

### Tips and Tricks

- Keep it simple :)
- Databases are namespaced, so name them with `$this->getNamespace()` from the parent Adapter class.
- Create indexes for `$id`, `$read`, and `$write` fields by default for query performance.
- Filter new IDs with `$this->filter($id);`
- Prioritize code performance.
- The Query and Queries validators contain the information about which queries the adapters support.
- The [Authorization validator](/src/Database/Validator/Authorization.php) is used to check permissions for searching methods `find()` and `sort()`. Ensure these methods only return documents with the correct `read` permissions.
- The `Database` class has useful constants like types and definitions. Prefer these constants when comparing strings.

#### SQL Databases
- Treat Collections as tables and Documents as rows, with attributes as columns. NoSQL databases are more straight-forward to translate.
- For row-level permissions, create a pair of tables: one for data and one for read/write permissions. The MariaDB adapter demonstrates the implementation.

#### NoSQL Databases
- NoSQL databases may not need to implement the attribute functions. See the MongoDB adapter as an example.
