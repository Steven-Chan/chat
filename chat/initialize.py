import skygear
from skygear.container import SkygearContainer
from skygear.options import options as skyoptions
from skygear.utils import db

from .field import Field
from .schema import Schema, SchemaHelper


def register_initialization_event_handlers(settings):
    def _base_message_fields():
        return [Field('attachment', 'asset'),
                Field('body', 'string'),
                Field('metadata', 'json'),
                Field('conversation', 'ref(conversation)'),
                Field('message_status', 'string'),
                Field('seq', 'sequence'),
                Field('revision', 'number'),
                Field('edited_by', 'ref(user)'),
                Field('edited_at', 'datetime')]

    def _message_schema():
        extra_fields = [Field('deleted', 'boolean'),
                        Field('previous_conversation_message',
                              'ref(message)')]
        fields = _base_message_fields() + extra_fields
        return Schema('message', fields)

    def _message_history_schema():
        fields = _base_message_fields() + [Field('parent', 'ref(message)')]
        return Schema('message_history', fields)

    @skygear.event("before-plugins-ready")
    def chat_plugin_init(config):
        container = SkygearContainer(api_key=skyoptions.masterkey)
        schema_helper = SchemaHelper(container)
        # We need this to provision the record type. Otherwise, make the follow
        # up `ref` type will fails.
        schema_helper.create([
            Schema('user', []),
            Schema('message', []),
            Schema('conversation', [])
        ], plugin_request=True)

        conversation_schema = Schema('conversation',
                                     [Field('title', 'string'),
                                      Field('metadata', 'json'),
                                      Field('deleted', 'boolean'),
                                      Field('distinct_by_participants',
                                            'boolean'),
                                      Field('last_message', 'ref(message)')])
        user_schema = Schema('user', [Field('name', 'string')])
        user_conversation_schema = Schema('user_conversation',
                                          [Field('user', 'ref(user)'),
                                           Field('conversation',
                                                 'ref(conversation)'),
                                           Field('unread_count',
                                                 'number'),
                                           Field('last_read_message',
                                                 'ref(message)'),
                                           Field('is_admin',
                                                 'boolean')])
        receipt_schema = Schema('receipt',
                                [Field('user', 'ref(user)'),
                                 Field('message', 'ref(message)'),
                                 Field('read_at', 'datetime'),
                                 Field('delivered_at', 'datetime')])
        message_schema = _message_schema()
        message_history_schema = _message_history_schema()
        schema_helper.create([user_schema,
                              user_conversation_schema,
                              conversation_schema,
                              message_schema,
                              message_history_schema,
                              receipt_schema],
                             plugin_request=True)

        with db.conn() as conn:
          populate_message_previous_message(conn)
          install_populate_previous_message_trigger(conn)


    def populate_message_previous_message(conn):
      stmt = '''
UPDATE message m
SET previous_conversation_message = m1.prev_msg
FROM (
    SELECT
        _id,
        FIRST_VALUE(_id) OVER w AS prev_msg
    FROM message
    WINDOW
        -- the window only contains messages of the same conversation
        -- and limited to 1 message only
        -- with smaller seq than current row
        w AS
        (
          PARTITION BY conversation
          ORDER BY seq DESC
          ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
        )
) AS m1
WHERE m._id = m1._id;
      '''

      conn.execute(stmt)


    def install_populate_previous_message_trigger(conn):
        stmt = '''
CREATE OR REPLACE FUNCTION update_message_previous_conversation_message()
RETURNS trigger AS $$
BEGIN
SET search_path TO app_my_skygear_app, public;

NEW.previous_conversation_message :=
(
    SELECT
        m._id
    FROM
    (
        SELECT
            _id,
            conversation,
            seq
        FROM message
        WHERE
            seq < NEW.seq AND
            conversation = NEW.conversation
        ORDER BY seq DESC
        LIMIT 1
    ) AS m
);

RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS insert_message_trigger on "message";
CREATE TRIGGER insert_message_trigger
    BEFORE INSERT ON "message"
    FOR EACH ROW
    EXECUTE PROCEDURE update_message_previous_conversation_message();
        '''

        conn.execute(stmt)
