from pyspark.sql.types import StructType, StructField, StringType, BooleanType

def get_schema_definition() -> StructType:
    """
    Creates a PySpark DataFrame schema from the provided JSON structure.
    """

    user_schema = StructType([
        StructField("_", StringType(), True),
        StructField("id", StringType(), True),  # Changed LongType to StringType because the id seems too large for a LongType, and it's represented as string in JSON
        StructField("is_self", BooleanType(), True),
        StructField("contact", BooleanType(), True),
        StructField("mutual_contact", BooleanType(), True),
        StructField("deleted", BooleanType(), True),
        StructField("bot", BooleanType(), True),
        StructField("bot_chat_history", BooleanType(), True),
        StructField("bot_nochats", BooleanType(), True),
        StructField("verified", BooleanType(), True),
        StructField("restricted", BooleanType(), True),
        StructField("min", BooleanType(), True),
        StructField("bot_inline_geo", BooleanType(), True),
        StructField("support", BooleanType(), True),
        StructField("scam", BooleanType(), True),
        StructField("apply_min_photo", BooleanType(), True),
        StructField("fake", BooleanType(), True),
        StructField("bot_attach_menu", BooleanType(), True),
        StructField("premium", BooleanType(), True),
        StructField("attach_menu_enabled", BooleanType(), True),
        StructField("bot_can_edit", BooleanType(), True),
        StructField("close_friend", BooleanType(), True),
        StructField("stories_hidden", BooleanType(), True),
        StructField("stories_unavailable", BooleanType(), True),
        StructField("contact_require_premium", BooleanType(), True),
        StructField("bot_business", BooleanType(), True),
        StructField("bot_has_main_app", BooleanType(), True),
        StructField("access_hash", StringType(), True), # access_hash is a large number, so StringType is more appropriate.
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("username", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("photo", StringType(), True),
        StructField("status", StringType(), True), # Complex structure, storing as string for simplicity. You can create a nested schema if needed.
        StructField("bot_info_version", StringType(), True),
        StructField("restriction_reason", StringType(), True), # changed to string because it contains "[]"
        StructField("bot_inline_placeholder", StringType(), True),
        StructField("lang_code", StringType(), True),
        StructField("emoji_status", StringType(), True),
        StructField("usernames", StringType(), True), # changed to string because it contains "[]"
        StructField("stories_max_id", StringType(), True),
        StructField("color", StringType(), True),
        StructField("profile_color", StringType(), True),
        StructField("bot_active_users", StringType(), True)
    ])

    schema = StructType([
        StructField("crawling_date", StringType(), True),
        StructField("message_date", StringType(), True),
        StructField("message_text", StringType(), True),
        StructField("dialog", StringType(), True),
        StructField("post_author", StringType(), True),
        StructField("is_channel", BooleanType(), True),
        StructField("is_group", BooleanType(), True),
        StructField("user", user_schema, True)
    ])

    return schema