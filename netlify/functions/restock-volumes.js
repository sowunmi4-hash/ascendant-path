const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MAX_STOCK = 10;
const RESTOCK_THRESHOLD = 2;

exports.handler = async (event) => {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Fetch all active volume items that have dropped below the threshold
  const { data: items, error: fetchError } = await supabase
    .schema("library")
    .from("library_store_items")
    .select("id, item_key, item_name, stock")
    .in("item_type", ["cultivation_volume", "bond_volume"])
    .eq("is_active", true)
    .lt("stock", RESTOCK_THRESHOLD);

  if (fetchError) {
    console.error("Restock fetch error:", fetchError.message);
    return { statusCode: 500 };
  }

  if (!items || items.length === 0) {
    console.log("All volumes above restock threshold — nothing to do.");
    return { statusCode: 200 };
  }

  const restocked = [];
  const failed = [];

  for (const item of items) {
    const quantityToAdd = MAX_STOCK - item.stock;

    const { error: updateError } = await supabase
      .schema("library")
      .from("library_store_items")
      .update({ stock: MAX_STOCK, updated_at: new Date().toISOString() })
      .eq("id", item.id);

    if (updateError) {
      console.error(`Failed to restock ${item.item_key}:`, updateError.message);
      failed.push(item.item_key);
      continue;
    }

    // Log the restock
    await supabase
      .schema("library")
      .from("library_restock_log")
      .insert({
        store_item_id: item.id,
        item_key: item.item_key,
        item_name: item.item_name,
        quantity_added: quantityToAdd,
        restocked_by: "auto_scheduler",
        notes: `Auto-restock triggered: stock dropped below ${RESTOCK_THRESHOLD}`,
      });

    restocked.push({ item_key: item.item_key, from: item.stock, to: MAX_STOCK });
  }

  console.log(`Restock complete. Restocked: ${restocked.length}, Failed: ${failed.length}`, restocked);
  return { statusCode: 200 };
};

module.exports.config = {
  schedule: "0 0 * * *",
};
