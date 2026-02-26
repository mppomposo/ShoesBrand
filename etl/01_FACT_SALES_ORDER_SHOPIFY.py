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

# Nombre de la tabla en Supabase (la crearás después)
TARGET_TABLE = "fact_sale_order_shopify" 

headers = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Accept': 'application/json'
}

# ----- FUNCIÓN PARA CONECTAR A SUPABASE -----
def get_supabase_client() -> Client:
    """Crea y retorna un cliente de Supabase"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ Faltan SUPABASE_URL o SUPABASE_KEY en las variables de entorno")
    
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Cliente de Supabase creado exitosamente")
        return supabase
    except Exception as e:
        print(f"❌ Error al crear cliente de Supabase: {e}")
        raise

# ----- FUNCIÓN PARA VOLCAR DATOS A SUPABASE (reemplaza a dump_to_postgres) -----
def upsert_to_supabase(df, table_name: str, batch_size: int = 100):
    """
    Inserta o actualiza datos en Supabase por lotes
    - df: DataFrame con los datos
    - table_name: nombre de la tabla en Supabase
    - batch_size: número de filas por lote (Supabase tiene límites)
    """
    print(f"\n💾 Iniciando volcado a Supabase - Tabla '{table_name}'...")
    
    # Limpiar columnas que puedan dar problemas
    df_clean = df.copy()
    
    # 1. Convertir NaN a None (JSON válido)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # 2. Convertir tipos no serializables
    for col in df_clean.select_dtypes(include=['datetime64[ns]']).columns:
        df_clean[col] = df_clean[col].astype(str)
    
    # 3. Asegurar que todos los valores son serializables a JSON
    registros = df_clean.to_dict(orient='records')
    
    try:
        supabase = get_supabase_client()
        
        # Verificar conexión con una consulta simple
        try:
            supabase.table(table_name).select("*").limit(1).execute()
        except Exception as e:
            print(f"⚠️ La tabla '{table_name}' podría no existir. Error: {e}")
            print("   Creando tabla automáticamente no es posible desde la API.")
            print("   Debes crearla manualmente en Supabase con esta estructura:")
            print("\n📋 ESTRUCTURA RECOMENDADA:")
            print("-" * 50)
            for col in df_clean.columns:
                ejemplo = df_clean[col].iloc[0] if len(df_clean) > 0 else "NULL"
                tipo = "text" if isinstance(ejemplo, str) else "numeric" if isinstance(ejemplo, (int, float)) else "timestamp"
                print(f"  {col}: {tipo}")
            print("-" * 50)
            
            respuesta = input("¿Quieres continuar con el volcado? (s/n): ")
            if respuesta.lower() != 's':
                return
        
        # Insertar por lotes
        total_registros = len(registros)
        print(f"   • Total registros a insertar: {total_registros:,}")
        
        for i in range(0, total_registros, batch_size):
            lote = registros[i:i+batch_size]
            
            try:
                # UPSERT: Si existe conflict en 'line_item_id' (único), actualiza
                # Nota: Debes crear una constraint UNIQUE en la tabla
                response = supabase.table(table_name).upsert(
                    lote, 
                    ignore_duplicates=False  # False = actualiza si existe
                ).execute()
                
                print(f"   ✅ Lote {i//batch_size + 1}/{(total_registros-1)//batch_size + 1}: {len(lote)} registros")
                
            except Exception as e:
                print(f"   ❌ Error en lote {i//batch_size + 1}: {e}")
                # Intentar insertar uno por uno para identificar el problema
                print("   🔍 Intentando insertar registro por registro...")
                for j, registro in enumerate(lote):
                    try:
                        supabase.table(table_name).insert(registro).execute()
                        print(f"      ✓ Registro {j+1} insertado")
                    except Exception as e2:
                        print(f"      ✗ Registro {j+1} falló: {e2}")
                        print(f"         Datos: {registro}")
                raise
        
        print(f"🎉 ¡Volcado completado exitosamente a Supabase!")
        print(f"   • Tabla: {table_name}")
        print(f"   • Registros: {total_registros:,}")
        
    except Exception as e:
        print(f"❌ Error crítico en volcado a Supabase: {e}")
        raise

# ----- TUS FUNCIONES EXISTENTES (SE MANTIENEN IGUAL) -----
def detectar_exchanges(df_combinado):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original exactamente igual) ...
    print("🔄 Detectando exchanges...")

    import re
    df = df_combinado.copy()
    count_pairs = 0
    count_lines = 0

    # Snapshot de los REFUND antes de mutar el df (para validar existencia del par)
    df_refund_snapshot = df[df["type"] == "REFUND"].copy()

    # 1) Ventas EXC-XXX aún como SALE
    exc_sales_to_classify = df[
        df['name'].str.startswith('EXC-', na=False) & (df['type'] == 'SALE')
    ]
    exc_names = exc_sales_to_classify['name'].unique()

    for exc_name in exc_names:
        # 2) base_number: EXC-15493-2 -> 15493
        m = re.search(r'EXC-(\d+)', exc_name)
        if not m:
            continue
        base_number = m.group(1)

        # 3) Validar que existe algún refund del mismo pedido base en el SNAPSHOT
        #    (#15493 o #15493-1, #15493-2, ...)
        refund_exists = df_refund_snapshot['name'].str.match(
            fr"^#{base_number}(-\d+)?$", na=False
        ).any()
        if not refund_exists:
            continue

        print(f"    • Exchange detectado: Venta {exc_name} ↔ Devolución #{base_number}*")
        count_pairs += 1

        # 4a) Reclasificar la venta EXC-XXX -> EXCHANGE (independiente de los refunds ya mutados)
        mask_exc_sale = (df['name'] == exc_name) & (df['type'] == 'SALE')
        if mask_exc_sale.any():
            df.loc[mask_exc_sale, 'type'] = 'EXCHANGE'
            count_lines += mask_exc_sale.sum()

        # 4b) Reclasificar SOLO las líneas de refund del mismo pedido base con FinalLineValue == 0
        #     (aunque en una iteración previa ya se hubieran convertido a EXCHANGE, volver a asignar es idempotente)
        mask_refund_name = df['name'].str.match(fr"^#{base_number}(-\d+)?$", na=False)
        mask_refund_zero = (df['FinalLineValue'] == 0)
        mask_refund_rows = mask_refund_name & mask_refund_zero & (df['type'] != 'SALE')
        if mask_refund_rows.any():
            df.loc[mask_refund_rows, 'type'] = 'EXCHANGE'
            count_lines += mask_refund_rows.sum()

    print(f"✅ {count_pairs} pares de intercambio detectados.")
    print(f"🔹 {count_lines} líneas reclasificadas como EXCHANGE.")

    return df

def get_all_orders_shopify():
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    all_orders = []
    since_id = None
    page_count = 0
    
    print("🚀 Iniciando extracción de pedidos (Réplica de Power Query)...")
    
    while True:
        try:
            # CONSTRUIR URL EXACTA COMO EN TU POWER QUERY
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

def expandir_datos_cliente(df_orders):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    print("👤 Expandiendo datos del cliente...")
    
    df_orders = df_orders.copy()
    
    # 1. Normalizar para obtener las columnas anidadas (ej. 'customer.email')
    df_temp = pd.json_normalize(df_orders.to_dict('records'))

    # Mapeo de columnas anidadas de cliente a nombres finales
    rename_map = {
        'customer.email': 'customer_email',
        'customer.first_name': 'customer_first_name',
        'customer.last_name': 'customer_last_name',
        'customer.id': 'customer_id' 
    }

    # Renombrar y asegurar que las columnas existan en df_orders
    for k_nested, k_final in rename_map.items():
        if k_nested in df_temp.columns:
            # Transferir datos
            df_orders[k_final] = df_temp[k_nested]
        elif k_final not in df_orders.columns:
            # Crear la columna si no existe
            df_orders[k_final] = None 

    # Eliminar la columna anidada original 'customer'
    df_orders.drop(columns=['customer'], inplace=True, errors='ignore')

    return df_orders

def expandir_direccion_envio(df_orders):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    import pandas as pd

    print("🏠 Expandiendo dirección de envío...")

    df_expanded = df_orders.copy()

    # --- 1️⃣ Expandir JSON de shipping_address si existe ---
    if 'shipping_address' in df_orders.columns and df_orders['shipping_address'].notna().any():
        df_shipping = pd.json_normalize(df_orders['shipping_address'].dropna().tolist())

        # NO añadimos un segundo prefijo: mantenemos "shipping_address.country", etc.
        df_shipping.index = df_orders[df_orders['shipping_address'].notna()].index

        # Unir datos normalizados
        df_expanded = df_expanded.join(df_shipping)

    # --- 2️⃣ Crear columnas finales ---
    df_expanded['shipping_province'] = None
    df_expanded['shipping_country'] = None

    # --- 3️⃣ Rellenar provincia ---
    for col in ['shipping_address.province', 'shipping_address.province_code']:
        if col in df_expanded.columns:
            df_expanded['shipping_province'] = df_expanded['shipping_province'].fillna(df_expanded[col])

    # --- 4️⃣ Rellenar país ---
    for col in ['shipping_address.country', 'shipping_address.country_name', 'shipping_address.country_code']:
        if col in df_expanded.columns:
            df_expanded['shipping_country'] = df_expanded['shipping_country'].fillna(df_expanded[col])

    # --- 5️⃣ Limpieza ---
    df_expanded['shipping_province'].replace('', None, inplace=True)
    df_expanded['shipping_country'].replace('', None, inplace=True)

    return df_expanded

def procesar_lineas_venta(df_orders):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    print("🛍️ Procesando líneas de venta...")
    
    lineas_venta = []
    
    for _, pedido in df_orders.iterrows():
        line_items = pedido.get('line_items', [])
    
        # Obtener valores de campos expandidos (ahora garantizados que existen)
        customer_email = pedido.get('customer_email', '')
        customer_first_name = pedido.get('customer_first_name', '')
        customer_last_name = pedido.get('customer_last_name', '')
        shipping_province = pedido.get('shipping_province', '')
        shipping_country = pedido.get('shipping_country', '')
        taxes_included = pedido.get('taxes_included', '')
        
        # Cálculo de shipping total
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
                 # 👉 IMPUESTOS REALES POR LÍNEA (Shopify)
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
            
            # Calcular LineValue y valores finales
            is_exchange_linea = linea.get('name', '').startswith('EXC-')
            if is_exchange_linea:
                linea['price'] = 0  # marcar la línea de exchange a cero euros
                linea['LineValue'] = (linea['price'] * linea['quantity']) - linea['discount_amount']
                linea['ShippingAllocated'] = shipping_allocated_per_item
                linea['FinalLineValue'] = linea['LineValue'] + linea['ShippingAllocated']
                linea['Neto_euros'] = 0
                linea['Bruto_euros'] =  0
                lineas_venta.append(linea)
                continue              
                
            else:
                linea['LineValue'] = (linea['price'] * linea['quantity']) - linea['discount_amount']
                linea['ShippingAllocated'] = shipping_allocated_per_item
                linea['FinalLineValue'] = linea['LineValue'] + linea['ShippingAllocated']

            if taxes_included:
                linea['Neto_euros'] = linea['FinalLineValue'] - linea ['tax_amount_shop'] - tax_shipping_allocated_per_item
                linea['Bruto_euros'] = linea['FinalLineValue']

            else:
                linea['Neto_euros'] = linea['FinalLineValue']
                linea['Bruto_euros'] =  linea['FinalLineValue'] + linea ['tax_amount_shop']


            
            lineas_venta.append(linea)
    
    return pd.DataFrame(lineas_venta)

def clean_note_text(note):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    if note is None:
        return ""
    cleaned = note.replace('–', '-')  # normaliza guion largo
    return cleaned

def extract_sku(line: str) -> str:
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    tokens = line.split()
    if not tokens:
        return ""

    first_token = tokens[0]

    # a) Si se repite la primera palabra, cortar antes de la segunda aparición
    repeat_idx = next(
        (i for i, t in enumerate(tokens[1:], start=1) if t.lower() == first_token.lower()),
        None
    )

    # b) Si hay token con "/", cortar antes de él
    slash_idx = next((i for i, t in enumerate(tokens) if "/" in t), None)

    # decidir el punto de corte
    cut = len(tokens)
    if repeat_idx is not None:
        cut = min(cut, repeat_idx)
    if slash_idx is not None:
        cut = min(cut, slash_idx)

    sku = " ".join(tokens[:cut]).strip()

    # c) eliminar posibles "The" al final (caso SOCKS)
    if sku.endswith(" The"):
        sku = sku[:-4].strip()

    return sku

def procesar_devoluciones(df_orders):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    print("🔄 Procesando devoluciones...")

    import re
    import pandas as pd

    lineas_devolucion = []
    DEBUG_ORDER_NAME = '#16333'

    # ---------------------------
    # Helpers: monedas a EUR (shop currency)
    # ---------------------------
    def infer_presentment_per_shop_rate(refund_line_items):
        """
        Devuelve ratio (presentment / shop) usando subtotal_set del primer refund_line_item válido.
        Ej: 255.00 AUD / 143.54 EUR = 1.776... (AUD por EUR)
        """
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
        """
        Devuelve el amount en shop currency (EUR):
        1) Si existe amount_set.shop_money.amount -> úsalo.
        2) Si no existe, convierte usando ratio implícito (EUR = presentment / (presentment_per_shop)).
        3) Si no hay ratio, devuelve t['amount'] tal cual (fallback).
        """
        amount_set = t.get('amount_set', {}) or {}
        shop_amt = amount_set.get('shop_money', {}).get('amount')
        if shop_amt is not None:
            try:
                return float(shop_amt)
            except (ValueError, TypeError):
                return 0.0

        # Fallback: convertir con ratio inferido
        try:
            amt = float(t.get('amount', 0) or 0)
        except (ValueError, TypeError):
            amt = 0.0

        if presentment_per_shop_rate and presentment_per_shop_rate != 0:
            return amt / presentment_per_shop_rate

        return amt

    # ---------------------------
    # Helper: sacar VAT rate a nivel pedido (para fallback neto/bruto)
    # ---------------------------
    def get_order_tax_rate(pedido):
        """
        Intenta obtener un rate tipo 0.21 desde:
        - pedido['tax_lines'][].rate  (ideal)
        Si no existe, devuelve None.
        """
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

        # Fallback VAT rate a nivel pedido (si procede)
        order_tax_rate = get_order_tax_rate(pedido)

        cleaned_note = clean_note_text(order_note)

        # --- 1️⃣ Buscar solo líneas con "- ... - exchange"
        exchange_lines = re.findall(
            r"^- (.+?)\s+-\s+exchange\s*$",
            cleaned_note,
            flags=re.MULTILINE | re.IGNORECASE
        )

        # --- 2️⃣ Extraer SKUs limpios usando extract_sku
        sku_exchange_list = [extract_sku(line) for line in exchange_lines if extract_sku(line)]

        # --- DEBUG
        if order_name == DEBUG_ORDER_NAME:
            print(f"\n--- DEBUG PEDIDO {DEBUG_ORDER_NAME} ---")
            print("Líneas detectadas como exchange:")
            for l in exchange_lines:
                print("•", l)
            print("SKUs exchange detectados:", sku_exchange_list)
            print("-------------------------------------")

        # --- 3️⃣ Procesar refunds ---
        for refund in refunds:
            refund_created_at = refund.get('created_at', pedido.get('created_at'))
            refund_line_items = refund.get('refund_line_items', []) or []

            # ✅ Inferir FX implícito (presentment/shop) desde las líneas del refund
            presentment_per_shop_rate = infer_presentment_per_shop_rate(refund_line_items)

            # ✅ TRANSACTIONS a EUR (shop currency)
            refund_transaction_amount_abs = sum(
                tx_amount_in_shop_currency(t, presentment_per_shop_rate)
                for t in (refund.get('transactions', []) or [])
                if t.get('kind') == 'refund'
            )
            refund_transaction_amount_net = -refund_transaction_amount_abs

            # ✅ Subtotales ya en EUR (shop_money)
            total_subtotal_refunded_cash_items = sum(
                float(item.get('subtotal_set', {}).get('shop_money', {}).get('amount', 0)) * (item.get('quantity', 0) or 0)
                for item in refund_line_items
                if item.get('line_item', {}).get('sku', '') not in sku_exchange_list
            )

            # ✅ discrepancy_charge en EUR (EUR - EUR)
            discrepancy_charge = total_subtotal_refunded_cash_items - refund_transaction_amount_abs
            if abs(discrepancy_charge) < 0.01:
                discrepancy_charge = 0
            elif discrepancy_charge < 0:
                discrepancy_charge = 0

            # Crear líneas finales
            if refund_line_items:
                for refund_line in refund_line_items:

                    # 🧹 Ignorar devoluciones que son cancelaciones previas al envío
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

                    line_sku =  (original_line.get('sku') or 
                    (f"{original_line.get('title', '')}-{original_line.get('variant_title', '')}" if original_line.get('variant_title') else original_line.get('title', ''))
                    )

                
                    
                    line_variant_id = str(original_line.get('variant_id', ''))
                    line_variant_title = str(original_line.get('variant_title', ''))

                    is_exchange_item = line_sku in sku_exchange_list
                    item_subtotal_value = price_subtotal_refunded * line_quantity  # negativo en refunds

                    # Impuestos por línea (si Shopify los trae)
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
                        # ✅ ahora refund_amount estará en EUR (shop currency)
                        'refund_amount': refund_transaction_amount_net,
                        'trace_status': 'OK',
                        'tax_amount': tax_amount_presentment,
                        'tax_amount_shop': tax_amount_shop,
                    }

                    # Valor cero si es exchange
                    if is_exchange_item:
                        linea['LineValue'] = 0.00
                        linea['ShippingAllocated'] = 0.00
                        linea['FinalLineValue'] = 0.00
                        # Neto/Bruto en 0
                        linea['Neto_euros'] = 0.00
                        linea['Bruto_euros'] = 0.00
                    else:
                        # Reparto del "ajuste no-producto" (tu discrepancy_charge)
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

                        # ---------------------------
                        # Neto / Bruto (EUR)
                        # ---------------------------
                        # Mantén bruto como el valor final (es lo que te cuadra perfecto en BI)
                        linea['Bruto_euros'] = linea['FinalLineValue']

                        if taxes_included:
                            # Caso 1: Shopify trae impuestos por línea -> usa tu lógica original
                            if abs(linea['tax_amount_shop']) > 0.0001:
                                # Tu convención original (mantengo tal cual)
                                #linea['Neto_euros'] = linea['FinalLineValue'] - linea['tax_amount_shop'] * (-1)
                                linea['Neto_euros'] = linea['Bruto_euros'] / (1 + order_tax_rate)
                            else:
                                # Caso 2 (fallback): NO hay tax_lines por línea, pero sí hay rate a nivel pedido
                                # Convertimos bruto->neto: neto = bruto / (1 + rate)
                                if order_tax_rate is not None and order_tax_rate > 0:
                                    linea['Neto_euros'] = linea['Bruto_euros'] / (1 + order_tax_rate)
                                else:
                                    # Sin rate: no podemos desglosar, igualamos
                                    linea['Neto_euros'] = linea['Bruto_euros']
                        else:
                            # taxes_included = False
                            # Tu convención original (mantengo tal cual)
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
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    print("🔗 Combinando ventas y devoluciones...")
    
    columnas_comunes = ['order_id', 'name', 'financial_status', 'created_at', 'customer_email', 
                        'customer_first_name', 'customer_last_name', 'shipping_province', 'shipping_country',
                        'line_item_id','variant_id','variant_title', 'sku', 'title', 'quantity', 'price', 'discount_amount', 
                        'LineValue', 'ShippingAllocated', 'FinalLineValue', 'type', 'source_name','Neto_euros','Bruto_euros']
    
    # Asegurar que todos los DataFrames tienen las columnas comunes antes de concatenar
    for col in columnas_comunes:
        if col not in df_ventas.columns:
            df_ventas[col] = None
        if col not in df_devoluciones.columns:
            df_devoluciones[col] = None
            
    # Seleccionar solo columnas comunes
    df_ventas_clean = df_ventas[columnas_comunes].copy()
    df_devoluciones_clean = df_devoluciones[columnas_comunes].copy()
    
    # Combinar
    df_combinado = pd.concat([df_ventas_clean, df_devoluciones_clean], ignore_index=True)
    
    # Ordenar por fecha
    df_combinado['created_at'] = pd.to_datetime(df_combinado['created_at'], errors='coerce')
    df_combinado = df_combinado.sort_values(['created_at', 'order_id']).reset_index(drop=True)
    
    return df_combinado

def clean_datetimes(df):
    """TU FUNCIÓN ORIGINAL - SIN CAMBIOS"""
    # ... (tu código original) ...
    df_clean = df.copy()
    
    print("Limpiando timezones...")
    
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            temp = pd.to_datetime(df_clean[col], errors='coerce')
            if pd.api.types.is_datetime64_any_dtype(temp) and temp.notna().sum() > (len(df_clean) / 2):
                df_clean[col] = temp
        
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            try:
                if df_clean[col].dt.tz is not None:
                    df_clean[col] = df_clean[col].dt.tz_localize(None)
                else:
                    df_clean[col] = df_clean[col].dt.tz_convert(None)
            except AttributeError:
                df_clean[col] = df_clean[col].apply(lambda x: x.replace(tzinfo=None) if pd.notna(x) and hasattr(x, 'tzinfo') and x.tzinfo is not None else x)
            except Exception as e:
                print(f"Advertencia: No se pudo limpiar el timezone en la columna {col}. Error: {e}")
                
    return df_clean

# 5. EJECUCIÓN PRINCIPAL
print("="*50)
print("=== APLICANDO LÓGICA DE POWER QUERY ===")
print("="*50)

# Extraer datos
all_orders = get_all_orders_shopify()

if all_orders:
    # Crear DataFrame base
    df_orders_base = pd.json_normalize(all_orders)
    
    print(f"\n🎉 EXTRACCIÓN COMPLETADA")
    print(f"✅ Total pedidos: {len(all_orders):,}")
    
    # APLICAR LÓGICA DE POWER QUERY PASO A PASO
    print("\n🔧 APLICANDO TRANSFORMACIONES...")
    
    # Paso 1 y 2: Expansión de datos
    df_con_cliente = expandir_datos_cliente(df_orders_base.copy())
    df_con_envio = expandir_direccion_envio(df_con_cliente)
    
    # Paso 3 y 4: Procesar líneas de venta y devolución
    df_ventas = procesar_lineas_venta(df_con_envio)
    df_devoluciones = procesar_devoluciones(df_con_envio)
    
    # Paso 5: Combinar todo
    df_final = combinar_datasets(df_ventas, df_devoluciones)
    
    # --- NUEVO PASO: DETECTAR EXCHANGES ---
    df_final = detectar_exchanges(df_final)
    
    # RESULTADOS
    print(f"\n🎯 TRANSFORMACIÓN COMPLETADA:")
    print(f"    • Total líneas combinadas: {len(df_final):,}")
    
    # Mostrar distribución de tipos
    distribucion_tipos = df_final['type'].value_counts()
    print(f"    • Distribución por tipo:")
    for tipo, cantidad in distribucion_tipos.items():
        print(f"      - {tipo}: {cantidad} líneas")
    
    # Mostrar muestra
    print(f"\n👀 MUESTRA DEL DATASET FINAL:")
    display(df_final[['name', 'sku', 'quantity', 'LineValue', 'FinalLineValue', 'type','Neto_euros','Bruto_euros']].head(10))
    
    # LIMPIAR TIMEZONES (NECESARIO PARA POSTGRESQL)
    df_final_clean = clean_datetimes(df_final)

    # ✅ Añadir columna last_updated
    df_final_clean['last_updated'] = datetime.now()
    
    # GUARDAR EN SUPABASE (reemplaza a dump_to_postgres)
    upsert_to_supabase(df_final_clean, TARGET_TABLE)

else:
    print("❌ No se obtuvieron pedidos")

print("\n" + "="*50)
print("🏁 PROCESO COMPLETADO")
print("="*50)
