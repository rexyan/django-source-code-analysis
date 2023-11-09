import re

from django.db.migrations.utils import get_migration_name_timestamp
from django.db.transaction import atomic

from .exceptions import IrreversibleError


# 迁移的抽象
class Migration:

    # 迁移的操作
    operations = []

    # 迁移的依赖
    dependencies = []

    run_before = []

    replaces = []

    initial = None

    atomic = True

    def __init__(self, name, app_label):
        self.name = name
        self.app_label = app_label
        # Copy dependencies & other attrs as we might mutate them at runtime
        self.operations = list(self.__class__.operations)
        self.dependencies = list(self.__class__.dependencies)
        self.run_before = list(self.__class__.run_before)
        self.replaces = list(self.__class__.replaces)

    def __eq__(self, other):
        return (
            isinstance(other, Migration)
            and self.name == other.name
            and self.app_label == other.app_label
        )

    def __repr__(self):
        return "<Migration %s.%s>" % (self.app_label, self.name)

    def __str__(self):
        return "%s.%s" % (self.app_label, self.name)

    def __hash__(self):
        return hash("%s.%s" % (self.app_label, self.name))

    # 执行 operations 状态的迁移
    def mutate_state(self, project_state, preserve=True):
        """
        Take a ProjectState and return a new one with the migration's
        operations applied to it. Preserve the original object state by
        default and return a mutated state from a copy.
        """
        new_state = project_state
        if preserve:
            new_state = project_state.clone()

        for operation in self.operations:
            operation.state_forwards(self.app_label, new_state)
        return new_state

    # 将 Migration 的操作应用到数据库中
    def apply(self, project_state, schema_editor, collect_sql=False):
        """
        Take a project_state representing all migrations prior to this one
        and a schema_editor for a live database and apply the migration
        in a forwards order.

        Return the resulting project state for efficient reuse by following
        Migrations.
        """
        for operation in self.operations:
            # If this operation cannot be represented as SQL, place a comment
            # there instead
            if collect_sql:
                schema_editor.collected_sql.append("--")
                schema_editor.collected_sql.append("-- %s" % operation.describe())
                schema_editor.collected_sql.append("--")
                if not operation.reduces_to_sql:
                    schema_editor.collected_sql.append(
                        "-- THIS OPERATION CANNOT BE WRITTEN AS SQL"
                    )
                    continue
                collected_sql_before = len(schema_editor.collected_sql)
            # Save the state before the operation has run
            old_state = project_state.clone()
            operation.state_forwards(self.app_label, project_state)
            # Run the operation
            atomic_operation = operation.atomic or (
                self.atomic and operation.atomic is not False
            )
            if not schema_editor.atomic_migration and atomic_operation:
                # Force a transaction on a non-transactional-DDL backend or an
                # atomic operation inside a non-atomic migration.
                with atomic(schema_editor.connection.alias):
                    operation.database_forwards(
                        self.app_label, schema_editor, old_state, project_state
                    )
            else:
                # Normal behaviour
                operation.database_forwards(
                    self.app_label, schema_editor, old_state, project_state
                )
            if collect_sql and collected_sql_before == len(schema_editor.collected_sql):
                schema_editor.collected_sql.append("-- (no-op)")
        return project_state

    # 上述 apply 方法的方向操作
    def unapply(self, project_state, schema_editor, collect_sql=False):
        """
        Take a project_state representing all migrations prior to this one
        and a schema_editor for a live database and apply the migration
        in a reverse order.

        The backwards migration process consists of two phases:

        1. The intermediate states from right before the first until right
           after the last operation inside this migration are preserved.
        2. The operations are applied in reverse order using the states
           recorded in step 1.
        """
        # Construct all the intermediate states we need for a reverse migration
        to_run = []
        new_state = project_state
        # Phase 1
        for operation in self.operations:
            # If it's irreversible, error out
            if not operation.reversible:
                raise IrreversibleError(
                    "Operation %s in %s is not reversible" % (operation, self)
                )
            # Preserve new state from previous run to not tamper the same state
            # over all operations
            new_state = new_state.clone()
            old_state = new_state.clone()
            operation.state_forwards(self.app_label, new_state)
            to_run.insert(0, (operation, old_state, new_state))

        # Phase 2
        for operation, to_state, from_state in to_run:
            if collect_sql:
                schema_editor.collected_sql.append("--")
                schema_editor.collected_sql.append("-- %s" % operation.describe())
                schema_editor.collected_sql.append("--")
                if not operation.reduces_to_sql:
                    schema_editor.collected_sql.append(
                        "-- THIS OPERATION CANNOT BE WRITTEN AS SQL"
                    )
                    continue
                collected_sql_before = len(schema_editor.collected_sql)
            atomic_operation = operation.atomic or (
                self.atomic and operation.atomic is not False
            )
            if not schema_editor.atomic_migration and atomic_operation:
                # Force a transaction on a non-transactional-DDL backend or an
                # atomic operation inside a non-atomic migration.
                with atomic(schema_editor.connection.alias):
                    operation.database_backwards(
                        self.app_label, schema_editor, from_state, to_state
                    )
            else:
                # Normal behaviour
                operation.database_backwards(
                    self.app_label, schema_editor, from_state, to_state
                )
            if collect_sql and collected_sql_before == len(schema_editor.collected_sql):
                schema_editor.collected_sql.append("-- (no-op)")
        return project_state

    # 生成迁移操作的名称
    def suggest_name(self):
        """
        Suggest a name for the operations this migration might represent. Names
        are not guaranteed to be unique, but put some effort into the fallback
        name to avoid VCS conflicts if possible.
        """
        if self.initial:
            return "initial"

        raw_fragments = [op.migration_name_fragment for op in self.operations]
        fragments = [re.sub(r"\W+", "_", name) for name in raw_fragments if name]

        if not fragments or len(fragments) != len(self.operations):
            return "auto_%s" % get_migration_name_timestamp()

        name = fragments[0]
        for fragment in fragments[1:]:
            new_name = f"{name}_{fragment}"
            if len(new_name) > 52:
                name = f"{name}_and_more"
                break
            name = new_name
        return name


class SwappableTuple(tuple):
    """
    Subclass of tuple so Django can tell this was originally a swappable
    dependency when it reads the migration file.
    """

    def __new__(cls, value, setting):
        self = tuple.__new__(cls, value)
        self.setting = setting
        return self


def swappable_dependency(value):
    """Turn a setting value into a dependency."""
    return SwappableTuple((value.split(".", 1)[0], "__first__"), value)
