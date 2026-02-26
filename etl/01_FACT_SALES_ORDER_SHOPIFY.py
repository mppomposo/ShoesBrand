# 1. INSTALACIÓN E IMPORTACIÓN
import requests
import pandas as pd
import base64
import json
import time
import os
import re  
from datetime import datetime
from supabase import create_client, Client

# Función de compatibilidad para 'display'
try:
    from IPython.display import display
except ImportError:
    def display(df):
        print(df.to_string())

# 2. CONFIGURACIÓN
SHOP_NAME = os.environ.get("SHOP_NAME")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
API_VERSION = os.environ.get("API_VERSION")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Nombre de la tabla en Supabase
TARGET_TABLE = "fact_sale_order_shopify" 

headers = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Accept': 'application/json'
}

# =====================================================================
# FUNCIONES DE CONEXIÓN Y VOLCADO A SUPABASE
# =====================================================================

def get_supabase_client() -> Client:
    """Crea y retorna un cliente de Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ Faltan SUPABASE_URL o SUPABASE_KEY en las variables de entorno")
    
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Cliente de Supabase creado exitosamente")
        return supabase
    except Exception as e:
        print(f"❌ Error al crear cliente de Supabase: {e}")
        raise


def sanitize_records_for_json(registros: list) -> list:
    """
    Convierte TODOS los valores no serializables a JSON en cada registro:
      - pd.Timestamp / datetime → string ISO 8601
      - pd.NaT / NaN / None     → None
      - np.int64/float64        → int/float nativos de Python
    """
    import numpy as np

    clean = []
    for row in registros:
        new_row = {}
        for k, v in row.items():
            # 1) NaT, NaN, None → None
            if v is None or (isinstance(v, float) and pd.isna(v)):
                new_row[k] = None
            elif isinstance(v, pd.NaT.__class__):
                new_row[k] = None
            # 2) Timestamps y datetimes → ISO string
            elif isinstance(v, (pd.Timestamp, datetime)):
                # Quitar timezone si lo tiene y convertir a string
                if hasattr(v, 'tzinfo') and v.tzinfo is not None:
                    v = v.tz_localize(None) if v.tzinfo is None else v.tz_convert(None)
                new_row[k] = v.isoformat()
            # 3) numpy int/float → Python nativo
            elif isinstance(v, (np.integer,)):
                new_row[k] = int(v)
            elif isinstance(v, (np.floating,)):
                new_row[k] = None if np.isnan(v) else float(v)
            elif isinstance(v, np.bool_):
                new_row[k] = bool(v)
            else:
                new_row[k] = v
        clean.append(new_row)
    return clean


def upsert_to_supabase(df, table_name: str, batch_size: int = 500):
    """
    Replica el comportamiento de dump_to_postgres con if_exists='replace':
      1. Borra todos los registros existentes en la tabla.
      2. Inserta los nuevos datos por lotes.
    
    - df: DataFrame con los datos ya limpios.
    - table_name: nombre de la tabla en Supabase.
    - batch_size: filas por lote (default 500).
    """
    print(f"\n💾 Iniciando volcado a Supabase - Tabla '{table_name}'...")

    # --- 1) Preparar registros serializables ---
    df_clean = df.copy()
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    registros = sanitize_records_for_json(df_clean.to_dict(orient='records'))

    total_registros = len(registros)
    print(f"   • Total registros a insertar: {total_registros:,}")

    try:
        supabase = get_supabase_client()

        # --- 2) Borrar datos existentes (equivalente a if_exists='replace') ---
        print(f"   🗑️  Vaciando tabla '{table_name}' (replace mode)...")
        try:
            # Supabase no tiene TRUNCATE vía client; usamos delete con filtro amplio
            supabase.table(table_name).delete().gte("order_id", 0).execute()
            print(f"   ✅ Tabla vaciada correctamente")
        except Exception as e:
            print(f"   ⚠️  No se pudo vaciar la tabla (puede estar vacía o no existir): {e}")

        # --- 3) Insertar por lotes ---
        errores_lote = 0
        for i in range(0, total_registros, batch_size):
            lote = registros[i:i + batch_size]
            lote_num = i // batch_size + 1
            total_lotes = (total_registros - 1) // batch_size + 1

            try:
                supabase.table(table_name).insert(lote).execute()
                print(f"   ✅ Lote {lote_num}/{total_lotes}: {len(lote)} registros")
            except Exception as e:
                errores_lote += 1
                print(f"   ❌ Error en lote {lote_num}/{total_lotes}: {e}")
                # Si falla un lote, intentar de uno en uno para localizar el registro problemático
                print(f"   🔍 Reintentando lote {lote_num} registro a registro...")
                for j, registro in enumerate(lote):
                    try:
                        supabase.table(table_name).insert(registro).execute()
                    except Exception as e2:
                        print(f"      ✗ Registro {i + j + 1} falló: {e2}")
                        print(f"         order_id={registro.get('order_id')}, "
                              f"name={registro.get('name')}, "
                              f"created_at={registro.get('created_at')}")

        if errores_lote == 0:
            print(f"\n🎉 ¡Volcado completado exitosamente a Supabase!")
        else:
            print(f"\n⚠️  Volcado completado con {errores_lote} lotes con errores")

        print(f"   • Tabla: {table_name}")
        print(f"   • Registros procesados: {total_registros:,}")

    except Exception as e:
        print(f"❌ Error crítico en volcado a Supabase: {e}")
        raise


# =====================================================================
# FUNCIONES DE EXTRACCIÓN SHOPIFY
# =====================================================================

def get_all_orders_shopify():
    """
    Réplica exacta de la lógica de Power Query para extraer todos los pedidos.
    """
    all_orders = []
    since_id = None
    page_count = 0
    
    print("🚀 Iniciando extracción de pedidos (Réplica de Power Query)...")
    
    while True:
        try:
            if since_id is None:
                url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json?limit=250&status=any&order=id asc"
            else:
                url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json?limit=250&status=any&order=id asc&since_id={since_id}"
            
            print(f"📄 Página {page_count + 1}...", end=" ")
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            orders = data.get('orders', [])
            
            if not orders:
                print("✅ No hay más pedidos")
                break
            
            all_orders.extend(orders)
            since_id = orders[-1]['id']
            page_count += 1
            
            print(f"✅ {len(orders)} pedidos | Total: {len(all_orders)}")
            time.sleep(0.3)
                
        except Exception as e:
            print(f"❌ Error al extraer pedidos: {e}")
            break
    
    return all_orders


# =====================================================================
# FUNCIONES DE TRANSFORMACIÓN (LÓGICA DE POWER QUERY)
# =====================================================================

def expandir_datos_cliente(df_orders):
    """
    Equivalente a ExpandCustomer en Power Query.
    Garantiza que las columnas de cliente existan.
    """
    print("👤 Expandiendo datos del cliente...")
    
    df_orders = df_orders.copy()
    
    df_temp = pd.json_normalize(df_orders.to_dict('records'))

    rename_map = {
        'customer.email': 'customer_email',
        'customer.first_name': 'customer_first_name',
        'customer.last_name': 'customer_last_name',
        'customer.id': 'customer_id' 
    }

    for k_nested, k_final in rename_map.items():
        if k_nested in df_temp.columns:
            df_orders[k_final] = df_temp[k_nested]
        elif k_final not in df_orders.columns:
            df_orders[k_final] = None 

    df_orders.drop(columns=['customer'], inplace=True, errors='ignore')

    return df_orders


def expandir_direccion_envio(df_orders):
    """
    Expande el campo 'shipping_address' del JSON de Shopify y crea:
      - shipping_province
      - shipping_country
    """
    print("🏠 Expandiendo dirección de envío...")

    df_expanded = df_orders.copy()

    if 'shipping_address' in df_orders.columns and df_orders['shipping_address'].notna().any():
        df_shipping = pd.json_normalize(df_orders['shipping_address'].dropna().tolist())
        df_shipping.index = df_orders[df_orders['shipping_address'].notna()].index
        df_expanded = df_expanded.join(df_shipping)

    df_expanded['shipping_province'] = None
    df_expanded['shipping_country'] = None

    for col in ['shipping_address.province', 'shipping_address.province_code']:
        if col in df_expanded.columns:
            df_expanded['shipping_province'] = df_expanded['shipping_province'].fillna(df_expanded[col])

    for col in ['shipping_address.country', 'shipping_address.country_name', 'shipping_address.country_code']:
        if col in df_expanded.columns:
            df_expanded['shipping_country'] = df_expanded['shipping_country'].fillna(df_expanded[col])

    df_expanded['shipping_province'].replace('', None, inplace=True)
    df_expanded['shipping_country'].replace('', None, inplace=True)

    return df_expanded


def procesar_lineas_venta(df_orders):
    """Equivalente a la sección LÍNEAS DE VENTA en Power Query."""
    print("🛍️ Procesando líneas de venta...")
    
    lineas_venta = []
    
    for _, pedido in df_orders.iterrows():
        line_items = pedido.get('line_items', [])
    
        customer_email = pedido.get('customer_email', '')
        customer_first_name = pedido.get('customer_first_name', '')
        customer_last_name = pedido.get('customer_last_name', '')
        shipping_province = pedido.get('shipping_province', '')
        shipping_country = pedido.get('shipping_country', '')
        taxes_included = pedido.get('taxes_included', '')
        
        shipping_lines = pedido.get('shipping_lines', [])
        shipping_total = sum(
            float(shipping.get('discounted_price', shipping.get('price', 0)))
            for shipping in shipping_lines
        )
        shipping_allocated_per_item = shipping_total / len(line_items) if line_items else 0

        tax_shipping_total = sum(
            float(tax.get('price_set', {}).get('shop_money', {}).get('amount', 0))
            for shipping in shipping_lines
            for tax in shipping.get('tax_lines', [])
        )        
        tax_shipping_allocated_per_item = tax_shipping_total / len(line_items) if line_items else 0
        
        for item in line_items:
            
            linea = {
                'order_id': pedido.get('id'),
                'name': pedido.get('name'),
                'financial_status': pedido.get('financial_status'),
                'created_at': pedido.get('created_at'),
                'customer_email': customer_email,
                'customer_first_name': customer_first_name,
                'customer_last_name': customer_last_name,
                'shipping_province': shipping_province,
                'shipping_country': shipping_country,
                'source_name': pedido.get('source_name', ''),
            
                'line_item_id': item.get('id'),
                'variant_id': str(item.get('variant_id', '')),
                'variant_title': str(item.get('variant_title', '')),
                'sku': (
                    item.get('sku') or 
                    (f"{item.get('title', '')}-{item.get('variant_title', '')}" if item.get('variant_title') else item.get('title', ''))
                ),                
                
                'title': item.get('title', ''),
                'quantity': item.get('quantity', 0),
                'price': float(item.get('price', 0)),
                'discount_amount': sum(float(disc.get('amount', 0)) for disc in item.get('discount_allocations', [])),
                'tax_amount': sum(
                    float(tax.get('price_set', {})
                              .get('presentment_money', {})
                              .get('amount', 0))
                    for tax in item.get('tax_lines', [])
                ),
                'tax_amount_shop': sum(
                    float(tax.get('price_set', {})
                              .get('shop_money', {})
                              .get('amount', 0))
                    for tax in item.get('tax_lines', [])
                ),
            
                'type': 'SALE'
            }
            
            is_exchange_linea = linea.get('name', '').startswith('EXC-')
            if is_exchange_linea:
                linea['price'] = 0
                linea['LineValue'] = (linea['price'] * linea['quantity']) - linea['discount_amount']
                linea['ShippingAllocated'] = shipping_allocated_per_item
                linea['FinalLineValue'] = linea['LineValue'] + linea['ShippingAllocated']
                linea['Neto_euros'] = 0
                linea['Bruto_euros'] = 0
                lineas_venta.append(linea)
                continue              
                
            else:
                linea['LineValue'] = (linea['price'] * linea['quantity']) - linea['discount_amount']
                linea['ShippingAllocated'] = shipping_allocated_per_item
                linea['FinalLineValue'] = linea['LineValue'] + linea['ShippingAllocated']

            if taxes_included:
                linea['Neto_euros'] = linea['FinalLineValue'] - linea['tax_amount_shop'] - tax_shipping_allocated_per_item
                linea['Bruto_euros'] = linea['FinalLineValue']
            else:
                linea['Neto_euros'] = linea['FinalLineValue']
                linea['Bruto_euros'] = linea['FinalLineValue'] + linea['tax_amount_shop']

            lineas_venta.append(linea)
    
    return pd.DataFrame(lineas_venta)


def clean_note_text(note):
    """Limpia la nota normalizando los guiones."""
    if note is None:
        return ""
    cleaned = note.replace('–', '-')
    return cleaned


def extract_sku(line: str) -> str:
    """Extrae el SKU aplicando la lógica de corte por repetición y separador (/)."""
    tokens = line.split()
    if not tokens:
        return ""

    first_token = tokens[0]

    repeat_idx = next(
        (i for i, t in enumerate(tokens[1:], start=1) if t.lower() == first_token.lower()),
        None
    )
    slash_idx = next((i for i, t in enumerate(tokens) if "/" in t), None)

    cut = len(tokens)
    if repeat_idx is not None:
        cut = min(cut, repeat_idx)
    if slash_idx is not None:
        cut = min(cut, slash_idx)

    sku = " ".join(tokens[:cut]).strip()

    if sku.endswith(" The"):
        sku = sku[:-4].strip()

    return sku


def procesar_devoluciones(df_orders):
    """Procesa refunds replicando la lógica completa del pipeline local."""
    print("🔄 Procesando devoluciones...")

    lineas_devolucion = []
    DEBUG_ORDER_NAME = '#16333'

    def infer_presentment_per_shop_rate(refund_line_items):
        for rli in refund_line_items or []:
            ss = rli.get('subtotal_set', {})
            shop_amt = ss.get('shop_money', {}).get('amount')
            pres_amt = ss.get('presentment_money', {}).get('amount')
            try:
                if shop_amt is not None and pres_amt is not None and float(shop_amt) != 0:
                    return float(pres_amt) / float(shop_amt)
            except (ValueError, TypeError):
                continue
        return None

    def tx_amount_in_shop_currency(t, presentment_per_shop_rate):
        amount_set = t.get('amount_set', {}) or {}
        shop_amt = amount_set.get('shop_money', {}).get('amount')
        if shop_amt is not None:
            try:
                return float(shop_amt)
            except (ValueError, TypeError):
                return 0.0

        try:
            amt = float(t.get('amount', 0) or 0)
        except (ValueError, TypeError):
            amt = 0.0

        if presentment_per_shop_rate and presentment_per_shop_rate != 0:
            return amt / presentment_per_shop_rate

        return amt

    def get_order_tax_rate(pedido):
        tax_lines = pedido.get('tax_lines', []) or []
        for tl in tax_lines:
            r = tl.get('rate')
            if r is None:
                continue
            try:
                return float(r)
            except (ValueError, TypeError):
                continue
        return None

    for _, pedido in df_orders.iterrows():
        refunds = pedido.get('refunds', [])
        order_name = pedido.get('name', '')
        order_note = pedido.get('note', '')
        taxes_included = pedido.get('taxes_included', False)

        order_tax_rate = get_order_tax_rate(pedido)
        cleaned_note = clean_note_text(order_note)

        exchange_lines = re.findall(
            r"^- (.+?)\s+-\s+exchange\s*$",
            cleaned_note,
            flags=re.MULTILINE | re.IGNORECASE
        )
        sku_exchange_list = [extract_sku(line) for line in exchange_lines if extract_sku(line)]

        if order_name == DEBUG_ORDER_NAME:
            print(f"\n--- DEBUG PEDIDO {DEBUG_ORDER_NAME} ---")
            print("Líneas detectadas como exchange:")
            for l in exchange_lines:
                print("•", l)
            print("SKUs exchange detectados:", sku_exchange_list)
            print("-------------------------------------")

        for refund in refunds:
            refund_created_at = refund.get('created_at', pedido.get('created_at'))
            refund_line_items = refund.get('refund_line_items', []) or []

            presentment_per_shop_rate = infer_presentment_per_shop_rate(refund_line_items)

            refund_transaction_amount_abs = sum(
                tx_amount_in_shop_currency(t, presentment_per_shop_rate)
                for t in (refund.get('transactions', []) or [])
                if t.get('kind') == 'refund'
            )
            refund_transaction_amount_net = -refund_transaction_amount_abs

            total_subtotal_refunded_cash_items = sum(
                float(item.get('subtotal_set', {}).get('shop_money', {}).get('amount', 0)) * (item.get('quantity', 0) or 0)
                for item in refund_line_items
                if item.get('line_item', {}).get('sku', '') not in sku_exchange_list
            )

            discrepancy_charge = total_subtotal_refunded_cash_items - refund_transaction_amount_abs
            if abs(discrepancy_charge) < 0.01:
                discrepancy_charge = 0
            elif discrepancy_charge < 0:
                discrepancy_charge = 0

            if refund_line_items:
                for refund_line in refund_line_items:

                    if refund_line.get("restock_type") == "cancel":
                        continue

                    line_item_id = refund_line.get('line_item_id')
                    original_line = next(
                        (item for item in (pedido.get('line_items', []) or []) if item.get('id') == line_item_id),
                        {}
                    )

                    price_subtotal_refunded = float(
                        refund_line.get('subtotal_set', {}).get('shop_money', {}).get('amount', 0) or 0
                    )
                    line_quantity = -(refund_line.get('quantity', 1) or 1)

                    line_sku = (original_line.get('sku') or 
                        (f"{original_line.get('title', '')}-{original_line.get('variant_title', '')}" if original_line.get('variant_title') else original_line.get('title', ''))
                    )
                    
                    line_variant_id = str(original_line.get('variant_id', ''))
                    line_variant_title = str(original_line.get('variant_title', ''))

                    is_exchange_item = line_sku in sku_exchange_list
                    item_subtotal_value = price_subtotal_refunded * line_quantity

                    tax_amount_presentment = sum(
                        float(tax.get('price_set', {}).get('presentment_money', {}).get('amount', 0) or 0)
                        for tax in (refund_line.get('line_item', {}) or {}).get('tax_lines', []) or []
                    )
                    tax_amount_shop = sum(
                        float(tax.get('price_set', {}).get('shop_money', {}).get('amount', 0) or 0)
                        for tax in (refund_line.get('line_item', {}) or {}).get('tax_lines', []) or []
                    )

                    linea = {
                        'order_id': pedido.get('id'),
                        'created_at': refund_created_at,
                        'name': order_name,
                        'financial_status': pedido.get('financial_status'),
                        'customer_email': pedido.get('customer_email', ''),
                        'customer_first_name': pedido.get('customer_first_name', ''),
                        'customer_last_name': pedido.get('customer_last_name', ''),
                        'shipping_province': pedido.get('shipping_province', ''),
                        'shipping_country': pedido.get('shipping_country', ''),
                        'source_name': pedido.get('source_name', ''),
                        'line_item_id': line_item_id,
                        'sku': line_sku,
                        'variant_id': line_variant_id,
                        'variant_title': line_variant_title,
                        'title': original_line.get('title', 'INFORMACION NO TRAZABLE'),
                        'quantity': line_quantity,
                        'price': price_subtotal_refunded,
                        'discount_amount': 0,
                        'type': 'REFUND',
                        'refund_amount': refund_transaction_amount_net,
                        'trace_status': 'OK',
                        'tax_amount': tax_amount_presentment,
                        'tax_amount_shop': tax_amount_shop,
                    }

                    if is_exchange_item:
                        linea['LineValue'] = 0.00
                        linea['ShippingAllocated'] = 0.00
                        linea['FinalLineValue'] = 0.00
                        linea['Neto_euros'] = 0.00
                        linea['Bruto_euros'] = 0.00
                    else:
                        if total_subtotal_refunded_cash_items > 0:
                            item_weight = (
                                (price_subtotal_refunded * (refund_line.get('quantity', 1) or 1))
                                / total_subtotal_refunded_cash_items
                            )
                            item_charge_distributed = discrepancy_charge * item_weight
                        else:
                            item_charge_distributed = 0

                        linea['LineValue'] = item_subtotal_value
                        linea['ShippingAllocated'] = item_charge_distributed
                        linea['FinalLineValue'] = linea['LineValue'] + linea['ShippingAllocated']

                        linea['Bruto_euros'] = linea['FinalLineValue']

                        if taxes_included:
                            if abs(linea['tax_amount_shop']) > 0.0001:
                                linea['Neto_euros'] = linea['Bruto_euros'] / (1 + order_tax_rate)
                            else:
                                if order_tax_rate is not None and order_tax_rate > 0:
                                    linea['Neto_euros'] = linea['Bruto_euros'] / (1 + order_tax_rate)
                                else:
                                    linea['Neto_euros'] = linea['Bruto_euros']
                        else:
                            linea['Neto_euros'] = linea['FinalLineValue']
                            linea['Bruto_euros'] = linea['FinalLineValue'] + linea['tax_amount_shop'] * (-1)

                    lineas_devolucion.append(linea)

    df_refunds = pd.DataFrame(lineas_devolucion)
    if not df_refunds.empty:
        df_refunds.loc[:, 'sku'] = df_refunds['sku'].fillna('NO DISPONIBLE')
        df_refunds.loc[:, 'variant_id'] = df_refunds['variant_id'].fillna('NO DISPONIBLE')
        df_refunds.loc[:, 'variant_title'] = df_refunds['variant_title'].fillna('NO DISPONIBLE')
        df_refunds.loc[:, 'title'] = df_refunds['title'].fillna('NO DISPONIBLE')
        df_refunds.loc[:, 'trace_status'] = df_refunds['trace_status'].fillna('NO DISPONIBLE')

    return df_refunds


def combinar_datasets(df_ventas, df_devoluciones):
    """Equivalente a la sección COMBINAR TODO en Power Query."""
    print("🔗 Combinando ventas y devoluciones...")
    
    columnas_comunes = [
        'order_id', 'name', 'financial_status', 'created_at', 'customer_email', 
        'customer_first_name', 'customer_last_name', 'shipping_province', 'shipping_country',
        'line_item_id', 'variant_id', 'variant_title', 'sku', 'title', 'quantity', 'price', 
        'discount_amount', 'LineValue', 'ShippingAllocated', 'FinalLineValue', 'type', 
        'source_name', 'Neto_euros', 'Bruto_euros'
    ]
    
    for col in columnas_comunes:
        if col not in df_ventas.columns:
            df_ventas[col] = None
        if col not in df_devoluciones.columns:
            df_devoluciones[col] = None
            
    df_ventas_clean = df_ventas[columnas_comunes].copy()
    df_devoluciones_clean = df_devoluciones[columnas_comunes].copy()
    
    df_combinado = pd.concat([df_ventas_clean, df_devoluciones_clean], ignore_index=True)
    
    df_combinado['created_at'] = pd.to_datetime(df_combinado['created_at'], errors='coerce')
    df_combinado = df_combinado.sort_values(['created_at', 'order_id']).reset_index(drop=True)
    
    return df_combinado


def detectar_exchanges(df_combinado):
    """
    Identifica pares de intercambio (SALE EXC-XXX y REFUND #XXX o #XXX-1, #XXX-2, ...)
    y reclasifica SOLO las líneas con FinalLineValue == 0 a 'EXCHANGE'.
    Mantiene las devoluciones reales (FinalLineValue ≠ 0) como 'REFUND'.
    """
    print("🔄 Detectando exchanges...")

    df = df_combinado.copy()
    count_pairs = 0
    count_lines = 0

    df_refund_snapshot = df[df["type"] == "REFUND"].copy()

    exc_sales_to_classify = df[
        df['name'].str.startswith('EXC-', na=False) & (df['type'] == 'SALE')
    ]
    exc_names = exc_sales_to_classify['name'].unique()

    for exc_name in exc_names:
        m = re.search(r'EXC-(\d+)', exc_name)
        if not m:
            continue
        base_number = m.group(1)

        refund_exists = df_refund_snapshot['name'].str.match(
            fr"^#{base_number}(-\d+)?$", na=False
        ).any()
        if not refund_exists:
            continue

        print(f"    • Exchange detectado: Venta {exc_name} ↔ Devolución #{base_number}*")
        count_pairs += 1

        mask_exc_sale = (df['name'] == exc_name) & (df['type'] == 'SALE')
        if mask_exc_sale.any():
            df.loc[mask_exc_sale, 'type'] = 'EXCHANGE'
            count_lines += mask_exc_sale.sum()

        mask_refund_name = df['name'].str.match(fr"^#{base_number}(-\d+)?$", na=False)
        mask_refund_zero = (df['FinalLineValue'] == 0)
        mask_refund_rows = mask_refund_name & mask_refund_zero & (df['type'] != 'SALE')
        if mask_refund_rows.any():
            df.loc[mask_refund_rows, 'type'] = 'EXCHANGE'
            count_lines += mask_refund_rows.sum()

    print(f"✅ {count_pairs} pares de intercambio detectados.")
    print(f"🔹 {count_lines} líneas reclasificadas como EXCHANGE.")

    return df


def clean_datetimes(df):
    """
    Elimina el timezone de TODAS las columnas datetime (tz-aware y tz-naive),
    preparándolas para serialización JSON y volcado a Supabase.
    """
    df_clean = df.copy()
    
    print("🕐 Limpiando timezones...")
    
    for col in df_clean.columns:
        # Intentar convertir columnas 'object' que parecen fechas
        if df_clean[col].dtype == 'object':
            temp = pd.to_datetime(df_clean[col], errors='coerce')
            if temp.notna().sum() > (len(df_clean) / 2):
                df_clean[col] = temp
        
        # Procesar TODAS las columnas datetime (con y sin timezone)
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            try:
                if hasattr(df_clean[col].dt, 'tz') and df_clean[col].dt.tz is not None:
                    # Columna con timezone → convertir a UTC y quitar tz
                    df_clean[col] = df_clean[col].dt.tz_convert('UTC').dt.tz_localize(None)
                # Si no tiene tz, no hace falta hacer nada
            except Exception:
                # Fallback: elemento a elemento (para columnas con tipos mixtos)
                df_clean[col] = df_clean[col].apply(
                    lambda x: x.tz_localize(None) if hasattr(x, 'tzinfo') and x.tzinfo is not None else x
                    if pd.notna(x) else x
                )
                
    return df_clean


# =====================================================================
# EJECUCIÓN PRINCIPAL
# =====================================================================

print("=" * 50)
print("=== PIPELINE ETL SHOPIFY → SUPABASE ===")
print("=" * 50)

# Extraer datos
all_orders = get_all_orders_shopify()

if all_orders:
    df_orders_base = pd.json_normalize(all_orders)
    
    print(f"\n🎉 EXTRACCIÓN COMPLETADA")
    print(f"✅ Total pedidos: {len(all_orders):,}")
    
    print("\n🔧 APLICANDO TRANSFORMACIONES...")
    
    # Paso 1 y 2: Expansión de datos
    df_con_cliente = expandir_datos_cliente(df_orders_base.copy())
    df_con_envio = expandir_direccion_envio(df_con_cliente)
    
    # Paso 3 y 4: Procesar líneas de venta y devolución
    df_ventas = procesar_lineas_venta(df_con_envio)
    df_devoluciones = procesar_devoluciones(df_con_envio)
    
    # Paso 5: Combinar todo
    df_final = combinar_datasets(df_ventas, df_devoluciones)
    
    # Paso 6: Detectar exchanges
    df_final = detectar_exchanges(df_final)
    
    # RESULTADOS
    print(f"\n🎯 TRANSFORMACIÓN COMPLETADA:")
    print(f"    • Total líneas combinadas: {len(df_final):,}")
    
    distribucion_tipos = df_final['type'].value_counts()
    print(f"    • Distribución por tipo:")
    for tipo, cantidad in distribucion_tipos.items():
        print(f"      - {tipo}: {cantidad} líneas")
    
    print(f"\n👀 MUESTRA DEL DATASET FINAL:")
    display(df_final[['name', 'sku', 'quantity', 'LineValue', 'FinalLineValue', 'type', 'Neto_euros', 'Bruto_euros']].head(10))
    
    # Limpiar timezones
    df_final_clean = clean_datetimes(df_final)

    # Añadir columna last_updated
    df_final_clean['last_updated'] = datetime.now().isoformat()
    
    # Normalizar nombres de columna a minúsculas (PostgreSQL/Supabase los almacena así)
    df_final_clean.columns = df_final_clean.columns.str.lower()
    
    # Volcar a Supabase
    upsert_to_supabase(df_final_clean, TARGET_TABLE)

else:
    print("❌ No se obtuvieron pedidos")

print("\n" + "=" * 50)
print("🏁 PROCESO COMPLETADO")
print("=" * 50)
