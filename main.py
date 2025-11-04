import os
from typing import List, Optional, Any, Dict
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP

app = FastMCP("Loans-db-server")

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        port=os.getenv("DB_PORT"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        cursor_factory=RealDictCursor
    )
    return conn

# ==================== CLIENTES ====================

@app.tool
def Add_client(name: str, email: str, phone: str) -> Dict[str, Any]:
    """Esta herramienta agrega un nuevo cliente"""
    try:
        if not name.strip() or not email.strip() or not phone.strip():
            return {"error": "El nombre, email y teléfono son obligatorios."}
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO clients (name, email, phone, createdate) VALUES (%s, %s, %s, %s) RETURNING id, name, email, phone, createdate",
            (name.strip(), email.strip(), phone.strip(), datetime.now().strftime('%Y-%m-%d'))
        )
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "success": True,
            "client": {
                "id": row["id"],
                "name": row["name"],
                "email": row["email"],
                "phone": row["phone"],
                "createdate": row["createdate"].strftime('%Y-%m-%d') if row["createdate"] else None
            }
        }
    except Exception as e:
        return {"error": f'Error al agregar un cliente: {str(e)}'}

@app.tool
def Get_clients() -> List[Dict[str, Any]]:
    """Esta herramienta obtiene la lista de clientes"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email, phone, createdate FROM clients")
        rows = cursor.fetchall()
        clients = []
        for row in rows:
            clients.append({
                "id": row["id"],
                "name": row["name"],
                "email": row["email"],
                "phone": row["phone"],
                "createdate": row["createdate"].strftime('%Y-%m-%d') if row["createdate"] else None
            })
        cursor.close()
        conn.close()
        return clients
    except Exception as e:
        return [{"error": f'Error al obtener clientes: {str(e)}'}]

@app.tool
def Get_client_by_id(client_id: int) -> Dict[str, Any]:
    """Obtiene la información de un cliente por su ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email, phone, createdate FROM clients WHERE id = %s", (client_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            return {"error": f"No se encontró el cliente con ID {client_id}"}
        
        return {
            "id": row["id"],
            "name": row["name"],
            "email": row["email"],
            "phone": row["phone"],
            "createdate": row["createdate"].strftime('%Y-%m-%d') if row["createdate"] else None
        }
    except Exception as e:
        return {"error": f'Error al obtener cliente: {str(e)}'}

# ==================== PRÉSTAMOS ====================

@app.tool
def Add_loan(
    client_id: int,
    original_amount: float,
    interest_rate: float,
    granting_date: Optional[str] = None,
    start_date: Optional[str] = None
) -> Dict[str, Any]:
    """Esta herramienta agrega un nuevo préstamo para un cliente"""
    try:
        if original_amount <= 0 or interest_rate < 0:
            return {"error": "El monto y la tasa de interés deben ser valores positivos."}
        if not granting_date:
            granting_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, name FROM clients WHERE id = %s", (client_id,))
        cliente = cursor.fetchone()
        if not cliente:
            cursor.close()
            conn.close()
            return {"error": f'No se encontró un cliente con ID {client_id}.'}

        # El saldo actual es igual al monto original al crear el préstamo
        cursor.execute(
            "INSERT INTO loans (client_id, original_amount, current_balance, granting_date, interest_rate, start_date, status) VALUES (%s, %s, %s, %s, %s, %s, 'active') RETURNING id",
            (client_id, original_amount, original_amount, granting_date, interest_rate, start_date)
        )
        row = cursor.fetchone()
        loan_id = row["id"]
        folio = f"F-{loan_id:07d}"

        # Actualizar el folio
        cursor.execute(
            "UPDATE loans SET folio = %s WHERE id = %s RETURNING id, client_id, original_amount, current_balance, granting_date, interest_rate, start_date, folio, status",
            (folio, loan_id)
        )
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "success": True,
            "loan": {
                "id": row["id"],
                "folio": row["folio"],
                "client": cliente["name"],
                "original_amount": float(row["original_amount"]),
                "current_balance": float(row["current_balance"]),
                "interest_rate": float(row["interest_rate"]),
                "granting_date": row["granting_date"].strftime('%Y-%m-%d') if row["granting_date"] else None,
                "start_date": row["start_date"].strftime('%Y-%m-%d') if row["start_date"] else None,
                "status": row["status"]
            }
        }
    except Exception as e:
        return {"error": f'Error al agregar un préstamo: {str(e)}'}

@app.tool
def Get_loans_by_client(client_id: int) -> List[Dict[str, Any]]:
    """Lista los préstamos de un cliente con saldos y folios"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, client_id, original_amount, current_balance, granting_date, 
                   interest_rate, start_date, folio, status
            FROM loans WHERE client_id = %s ORDER BY id DESC
        """, (client_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        loans = []
        for row in rows:
            loans.append({
                "id": row["id"],
                "client_id": row["client_id"],
                "folio": row["folio"],
                "original_amount": float(row["original_amount"]),
                "current_balance": float(row["current_balance"]),
                "interest_rate": float(row["interest_rate"]),
                "granting_date": row["granting_date"].strftime('%Y-%m-%d') if row["granting_date"] else None,
                "start_date": row["start_date"].strftime('%Y-%m-%d') if row["start_date"] else None,
                "status": row["status"]
            })
        return loans
    except Exception as e:
        return [{"error": f"Error en Get_loans_by_client: {str(e)}"}]

@app.tool
def Get_loan_by_id(loan_id: int) -> Dict[str, Any]:
    """Obtiene la información detallada de un préstamo por su ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.id, l.client_id, l.original_amount, l.current_balance, l.granting_date,
                   l.interest_rate, l.start_date, l.folio, l.status, c.name as client_name
            FROM loans l
            JOIN clients c ON l.client_id = c.id
            WHERE l.id = %s
        """, (loan_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            return {"error": f"No se encontró el préstamo con ID {loan_id}"}
        
        return {
            "id": row["id"],
            "client_id": row["client_id"],
            "client_name": row["client_name"],
            "folio": row["folio"],
            "original_amount": float(row["original_amount"]),
            "current_balance": float(row["current_balance"]),
            "interest_rate": float(row["interest_rate"]),
            "granting_date": row["granting_date"].strftime('%Y-%m-%d') if row["granting_date"] else None,
            "start_date": row["start_date"].strftime('%Y-%m-%d') if row["start_date"] else None,
            "status": row["status"]
        }
    except Exception as e:
        return {"error": f'Error al obtener préstamo: {str(e)}'}

# ==================== ESTADOS DE CUENTA ====================

@app.tool
def Generate_monthly_cutoff(loan_id: int, due_days: int = 10) -> Dict[str, Any]:
    """
    Genera el corte mensual para un préstamo:
    - La fecha de corte se calcula automáticamente: mismo día que el start_date, mes/año actual.
    - Calcula interés del periodo sobre current_balance a la fecha de corte.
    - Inserta movement 'interest_charge'.
    - Crea/inserta statements (period, saldos, interés generado, fechas).
    - Evita duplicados por periodo (si ya existe, retorna error).
    - NO genera statement si el periodo coincide con el mes del start_date.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1) Validaciones y obtención del préstamo
        cursor.execute("""
            SELECT id, client_id, original_amount, current_balance, interest_rate, start_date, status
            FROM loans WHERE id = %s
        """, (loan_id,))
        loan = cursor.fetchone()
        if not loan:
            cursor.close()
            conn.close()
            return {"error": f"No existe el préstamo {loan_id}."}
        
        if loan["status"] != 'active':
            cursor.close()
            conn.close()
            return {"error": f"El préstamo {loan_id} no está activo (status: {loan['status']})."}

        # 2) Determinar periodo actual y fecha de corte automática
        today = datetime.now().date()
        period = today.strftime("%Y-%m")  # Ejemplo: "2025-10"
        start_day = loan["start_date"].day
        cutoff_dt = today.replace(day=start_day)
        # Si el mes actual no tiene ese día (ej. 31 en febrero), usar el último día del mes
        try:
            cutoff_dt = today.replace(day=start_day)
        except ValueError:
            # Día fuera de rango, usar último día del mes
            next_month = today.replace(day=28) + timedelta(days=4)
            last_day = (next_month - timedelta(days=next_month.day)).day
            cutoff_dt = today.replace(day=last_day)

        start_period = loan["start_date"].strftime("%Y-%m")
        if period == start_period:
            cursor.close()
            conn.close()
            return {
                "success": True,
                "skipped": True,
                "reason": "skipped_same_month_as_start",
                "message": "Se omite generar estado de cuenta en el mismo mes del start_date. El primer corte será el mes siguiente.",
                "loan_id": loan_id,
                "period": period
            }
        
        # Fecha de vencimiento = fecha de corte + due_days
        due_date = cutoff_dt + timedelta(days=due_days)

        # 3) Rechazar si ya existe statement del periodo
        cursor.execute("""
            SELECT id FROM statements WHERE loan_id = %s AND period = %s
        """, (loan_id, period))
        existing = cursor.fetchone()
        if existing:
            cursor.close()
            conn.close()
            return {"error": f"Ya existe un estado de cuenta para el periodo {period} del préstamo {loan_id}."}

        current_balance = float(loan["current_balance"])
        interest_rate = float(loan["interest_rate"])  # mensual %
        interest_generated = round(current_balance * (interest_rate / 100.0), 2)

        # 4) Insertar movimiento de cargo de interés (no cambia saldo capital)
        cursor.execute("""
            INSERT INTO movements (
                loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
            )
            VALUES (%s, 'interest_charge', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            loan_id,
            interest_generated,
            current_balance,
            current_balance,
            cutoff_dt,
            period,
            f"INT-{period}",
            "Cargo de interés mensual"
        ))
        movement_row = cursor.fetchone()

        # 5) Insertar statement
        cursor.execute("""
            INSERT INTO statements (
                loan_id, period, initial_balance, final_balance,
                interest_generated, interest_paid, principal_paid,
                cut_off_date, due_date, status
            )
            VALUES (%s, %s, %s, %s, %s, 0, 0, %s, %s, 'pending')
            RETURNING id, period, initial_balance, final_balance, interest_generated, 
                      cut_off_date, due_date, status
        """, (
            loan_id, period,
            current_balance,  # saldo capital inicial del periodo
            current_balance,  # no varía por cargo de interés
            interest_generated,
            cutoff_dt, due_date
        ))
        statement_row = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "loan_id": loan_id,
            "period": period,
            "interest_generated": float(interest_generated),
            "statement_id": statement_row["id"],
            "interest_charge_movement_id": movement_row["id"],
            "statement": {
                "id": statement_row["id"],
                "period": statement_row["period"],
                "initial_balance": float(statement_row["initial_balance"]),
                "final_balance": float(statement_row["final_balance"]),
                "interest_generated": float(statement_row["interest_generated"]),
                "cut_off_date": statement_row["cut_off_date"].strftime('%Y-%m-%d'),
                "due_date": statement_row["due_date"].strftime('%Y-%m-%d'),
                "status": statement_row["status"]
            }
        }
    except Exception as e:
        return {"error": f"Error en Generate_monthly_cutoff: {str(e)}"}

@app.tool
def Generate_statements_for_active_loans(cutoff_date: Optional[str] = None, due_days: int = 10) -> Dict[str, Any]:
    """
    Genera estados de cuenta mensuales para TODOS los préstamos activos.
    - Itera sobre todos los préstamos con status='active'
    - Para cada uno, genera el corte mensual si no existe ya
    - NO genera cortes el mismo mes del start_date (Opción A)
    - Retorna resumen de éxitos y errores
    
    cutoff_date: 'YYYY-MM-DD' (si no se pasa, usa hoy)
    due_days: días después del corte para fecha de vencimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Determinar fecha de corte y periodo
        cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d").date() if cutoff_date else datetime.now().date()
        period = cutoff_dt.strftime("%Y-%m")
        
        # Obtener todos los préstamos activos
        cursor.execute("""
            SELECT id, client_id, current_balance, interest_rate, folio, start_date
            FROM loans 
            WHERE status = 'active'
            ORDER BY id
        """)
        active_loans = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        results = {
            "success": True,
            "period": period,
            "cutoff_date": cutoff_dt.strftime('%Y-%m-%d'),
            "total_loans": len(active_loans),
            "generated": 0,
            "skipped": 0,
            "errors": 0,
            "details": []
        }
        
        if not active_loans:
            return results
        
        for loan in active_loans:
            loan_id = loan["id"]
            folio = loan["folio"]
            start_period = loan["start_date"].strftime("%Y-%m")
            
            # Opción A: saltar si es el mismo mes del start_date
            if period == start_period:
                results["skipped"] += 1
                results["details"].append({
                    "loan_id": loan_id,
                    "folio": folio,
                    "status": "skipped",
                    "reason": "skipped_same_month_as_start"
                })
                continue
            
            # Llamar a Generate_monthly_cutoff para cada préstamo
            result = Generate_monthly_cutoff(
                loan_id=loan_id,
                cutoff_date=cutoff_dt.strftime('%Y-%m-%d'),
                due_days=due_days
            )
            
            if "error" in result:
                # Si el error es por duplicado, contar como skipped
                if "Ya existe un estado de cuenta" in result["error"]:
                    results["skipped"] += 1
                    results["details"].append({
                        "loan_id": loan_id,
                        "folio": folio,
                        "status": "skipped",
                        "reason": "already_exists"
                    })
                else:
                    results["errors"] += 1
                    results["details"].append({
                        "loan_id": loan_id,
                        "folio": folio,
                        "status": "error",
                        "message": result["error"]
                    })
            else:
                # Si Generate_monthly_cutoff devolvió skipped_same_month_as_start por validación interna
                if result.get("skipped") and result.get("reason") == "skipped_same_month_as_start":
                    results["skipped"] += 1
                    results["details"].append({
                        "loan_id": loan_id,
                        "folio": folio,
                        "status": "skipped",
                        "reason": "skipped_same_month_as_start"
                    })
                else:
                    results["generated"] += 1
                    results["details"].append({
                        "loan_id": loan_id,
                        "folio": folio,
                        "status": "generated",
                        "statement_id": result.get("statement_id"),
                        "interest_generated": result.get("interest_generated")
                    })
        
        return results
        
    except Exception as e:
        return {"error": f"Error en Generate_statements_for_active_loans: {str(e)}"}

@app.tool
def Get_loan_statements(loan_id: int, period: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Obtiene estados de cuenta de un préstamo. Si se pasa 'period' (YYYY-MM), filtra por ese periodo.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if period:
            cursor.execute("""
                SELECT id, loan_id, period, initial_balance, final_balance, interest_generated,
                       interest_paid, principal_paid, late_fee_generated, cut_off_date, due_date, status
                FROM statements
                WHERE loan_id = %s AND period = %s
                ORDER BY period DESC
            """, (loan_id, period))
        else:
            cursor.execute("""
                SELECT id, loan_id, period, initial_balance, final_balance, interest_generated,
                       interest_paid, principal_paid, late_fee_generated, cut_off_date, due_date, status
                FROM statements
                WHERE loan_id = %s
                ORDER BY period DESC
            """, (loan_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        statements = []
        for row in rows:
            statements.append({
                "id": row["id"],
                "loan_id": row["loan_id"],
                "period": row["period"],
                "initial_balance": float(row["initial_balance"]),
                "final_balance": float(row["final_balance"]),
                "interest_generated": float(row["interest_generated"]),
                "interest_paid": float(row["interest_paid"]),
                "principal_paid": float(row["principal_paid"]),
                "late_fee_generated": float(row["late_fee_generated"]),
                "cut_off_date": row["cut_off_date"].strftime('%Y-%m-%d'),
                "due_date": row["due_date"].strftime('%Y-%m-%d'),
                "status": row["status"]
            })
        return statements
    except Exception as e:
        return [{"error": f"Error en Get_loan_statements: {str(e)}"}]



# ==================== PAGOS ====================

@app.tool
def Register_interest_payment(
    loan_id: int, 
    period: str, 
    amount: float, 
    payment_date: Optional[str] = None, 
    reference: Optional[str] = None, 
    note: Optional[str] = None
) -> Dict[str, Any]:
    """
    Registra un pago de intereses para un periodo (formato period 'YYYY-MM').
    - Inserta movement 'interest_payment' (no cambia current_balance)
    - Actualiza statement: interest_paid y status
    """
    try:
        if amount <= 0:
            return {"error": "El monto debe ser mayor a 0."}

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, current_balance FROM loans WHERE id = %s", (loan_id,))
        loan = cursor.fetchone()
        if not loan:
            cursor.close()
            conn.close()
            return {"error": f"No existe el préstamo {loan_id}."}

        cursor.execute("""
            SELECT id, interest_generated, interest_paid, status
            FROM statements
            WHERE loan_id = %s AND period = %s
        """, (loan_id, period))
        stmt = cursor.fetchone()
        if not stmt:
            cursor.close()
            conn.close()
            return {"error": f"No existe statement para loan_id={loan_id}, period={period}. Genera el corte primero."}

        new_interest_paid = round(float(stmt["interest_paid"]) + amount, 2)
        interest_generated = float(stmt["interest_generated"])

        pay_date = datetime.strptime(payment_date, "%Y-%m-%d").date() if payment_date else datetime.now().date()

        # Movimiento de pago de interés
        prev_bal = float(loan["current_balance"])
        cursor.execute("""
            INSERT INTO movements (
                loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
            ) VALUES (%s, 'interest_payment', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            loan_id, amount, prev_bal, prev_bal,
            pay_date, period, reference, note or "Pago de interés"
        ))
        mov = cursor.fetchone()

        # Actualizar statement
        new_status = "paid" if abs(new_interest_paid - interest_generated) < 0.01 else ("partial" if new_interest_paid > 0 else stmt["status"])
        cursor.execute("""
            UPDATE statements
            SET interest_paid = %s, status = %s
            WHERE id = %s
            RETURNING id, period, interest_generated, interest_paid, principal_paid, status
        """, (new_interest_paid, new_status, stmt["id"]))
        updated_stmt = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "movement_id": mov["id"],
            "statement": {
                "id": updated_stmt["id"],
                "period": updated_stmt["period"],
                "interest_generated": float(updated_stmt["interest_generated"]),
                "interest_paid": float(updated_stmt["interest_paid"]),
                "principal_paid": float(updated_stmt["principal_paid"]),
                "status": updated_stmt["status"]
            }
        }
    except Exception as e:
        return {"error": f"Error en Register_interest_payment: {str(e)}"}

@app.tool
def Register_principal_payment(
    loan_id: int, 
    amount: float, 
    payment_date: Optional[str] = None, 
    reference: Optional[str] = None, 
    note: Optional[str] = None
) -> Dict[str, Any]:
    """
    Registra un abono a capital:
    - Inserta movement 'principal_payment'
    - Disminuye loans.current_balance
    - No toca intereses; reduce base para próximo corte
    - Si el saldo llega a 0, cambia el status del préstamo a 'closed'
    """
    try:
        if amount <= 0:
            return {"error": "El monto debe ser mayor a 0."}

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, current_balance, status FROM loans WHERE id = %s FOR UPDATE", (loan_id,))
        loan = cursor.fetchone()
        if not loan:
            cursor.close()
            conn.close()
            return {"error": f"No existe el préstamo {loan_id}."}

        prev_balance = float(loan["current_balance"])
        if amount > prev_balance:
            cursor.close()
            conn.close()
            return {"error": f"El abono ({amount}) no puede exceder el saldo actual ({prev_balance})."}

        new_balance = round(prev_balance - amount, 2)
        pay_date = datetime.strptime(payment_date, "%Y-%m-%d").date() if payment_date else datetime.now().date()

        cursor.execute("""
            INSERT INTO movements (
                loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
            ) VALUES (%s, 'principal_payment', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            loan_id, amount, prev_balance, new_balance,
            pay_date, pay_date.strftime("%Y-%m"), reference, note or "Abono a capital"
        ))
        mov = cursor.fetchone()

        # Actualizar saldo del préstamo
        new_status = 'closed' if new_balance == 0 else loan["status"]
        cursor.execute("""
            UPDATE loans SET current_balance = %s, status = %s 
            WHERE id = %s 
            RETURNING id, current_balance, status, folio
        """, (new_balance, new_status, loan_id))
        updated_loan = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "movement_id": mov["id"],
            "loan": {
                "id": updated_loan["id"],
                "folio": updated_loan["folio"],
                "current_balance": float(updated_loan["current_balance"]),
                "status": updated_loan["status"]
            },
            "message": "Préstamo liquidado y cerrado." if new_balance == 0 else "Abono a capital registrado."
        }
    except Exception as e:
        return {"error": f"Error en Register_principal_payment: {str(e)}"}

# ==================== MOVIMIENTOS ====================

@app.tool
def Get_loan_movements(loan_id: int, movement_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lista movimientos de un préstamo. Filtra opcionalmente por movement_type.
    movement_type ∈ {'interest_payment','principal_payment','interest_charge','late_fee_charge','adjustment'}
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if movement_type:
            cursor.execute("""
                SELECT id, loan_id, movement_type, amount, previous_balance, new_balance,
                       movement_date, application_period, reference, note
                FROM movements
                WHERE loan_id = %s AND movement_type = %s
                ORDER BY movement_date DESC, id DESC
            """, (loan_id, movement_type))
        else:
            cursor.execute("""
                SELECT id, loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
                FROM movements
                WHERE loan_id = %s
                ORDER BY movement_date DESC, id DESC
            """, (loan_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        movements = []
        for row in rows:
            movements.append({
                "id": row["id"],
                "loan_id": row["loan_id"],
                "movement_type": row["movement_type"],
                "amount": float(row["amount"]),
                "previous_balance": float(row["previous_balance"]),
                "new_balance": float(row["new_balance"]),
                "movement_date": row["movement_date"].strftime('%Y-%m-%d'),
                "application_period": row["application_period"],
                "reference": row["reference"],
                "note": row["note"]
            })
        return movements
    except Exception as e:
        return [{"error": f"Error en Get_loan_movements: {str(e)}"}]

# ==================== MORA Y CARGOS ====================

@app.tool
def Generate_late_fee(loan_id: int, period: str, late_fee_amount: float, charge_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Genera un cargo por mora para un periodo específico.
    - Inserta movement 'late_fee_charge'
    - Actualiza statement: late_fee_generated y status a 'overdue'
    """
    try:
        if late_fee_amount <= 0:
            return {"error": "El monto de mora debe ser mayor a 0."}

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe el préstamo
        cursor.execute("SELECT id, current_balance FROM loans WHERE id = %s", (loan_id,))
        loan = cursor.fetchone()
        if not loan:
            cursor.close()
            conn.close()
            return {"error": f"No existe el préstamo {loan_id}."}

        # Verificar que existe el statement
        cursor.execute("""
            SELECT id, late_fee_generated, status, due_date
            FROM statements
            WHERE loan_id = %s AND period = %s
        """, (loan_id, period))
        stmt = cursor.fetchone()
        if not stmt:
            cursor.close()
            conn.close()
            return {"error": f"No existe statement para loan_id={loan_id}, period={period}."}

        charge_dt = datetime.strptime(charge_date, "%Y-%m-%d").date() if charge_date else datetime.now().date()
        
        # Insertar movimiento de cargo por mora
        prev_bal = float(loan["current_balance"])
        cursor.execute("""
            INSERT INTO movements (
                loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
            ) VALUES (%s, 'late_fee_charge', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            loan_id, late_fee_amount, prev_bal, prev_bal,
            charge_dt, period, f"MORA-{period}", "Cargo por mora"
        ))
        mov = cursor.fetchone()

        # Actualizar statement
        new_late_fee = round(float(stmt["late_fee_generated"]) + late_fee_amount, 2)
        cursor.execute("""
            UPDATE statements
            SET late_fee_generated = %s, status = 'overdue'
            WHERE id = %s
            RETURNING id, period, late_fee_generated, status
        """, (new_late_fee, stmt["id"]))
        updated_stmt = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "movement_id": mov["id"],
            "statement": {
                "id": updated_stmt["id"],
                "period": updated_stmt["period"],
                "late_fee_generated": float(updated_stmt["late_fee_generated"]),
                "status": updated_stmt["status"]
            }
        }
    except Exception as e:
        return {"error": f"Error en Generate_late_fee: {str(e)}"}

@app.tool
def Check_overdue_statements(check_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Revisa todos los statements con status 'pending' o 'partial' cuya fecha de vencimiento ya pasó.
    Retorna lista de statements vencidos que requieren atención.
    
    check_date: 'YYYY-MM-DD' (si no se pasa, usa hoy)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        check_dt = datetime.strptime(check_date, "%Y-%m-%d").date() if check_date else datetime.now().date()
        
        cursor.execute("""
            SELECT s.id, s.loan_id, s.period, s.interest_generated, s.interest_paid,
                   s.late_fee_generated, s.due_date, s.status, l.folio, l.client_id, c.name as client_name
            FROM statements s
            JOIN loans l ON s.loan_id = l.id
            JOIN clients c ON l.client_id = c.id
            WHERE s.status IN ('pending', 'partial')
              AND s.due_date < %s
            ORDER BY s.due_date ASC
        """, (check_dt,))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        overdue = []
        for row in rows:
            days_overdue = (check_dt - row["due_date"]).days
            pending_interest = float(row["interest_generated"]) - float(row["interest_paid"])
            
            overdue.append({
                "statement_id": row["id"],
                "loan_id": row["loan_id"],
                "folio": row["folio"],
                "client_id": row["client_id"],
                "client_name": row["client_name"],
                "period": row["period"],
                "due_date": row["due_date"].strftime('%Y-%m-%d'),
                "days_overdue": days_overdue,
                "interest_generated": float(row["interest_generated"]),
                "interest_paid": float(row["interest_paid"]),
                "pending_interest": pending_interest,
                "late_fee_generated": float(row["late_fee_generated"]),
                "status": row["status"]
            })
        
        return overdue
    except Exception as e:
        return [{"error": f"Error en Check_overdue_statements: {str(e)}"}]

# ==================== CIERRE DE PRÉSTAMOS ====================

@app.tool
def Close_loan_if_zero(loan_id: int, close_date: Optional[str] = None, note: Optional[str] = None) -> Dict[str, Any]:
    """
    Cierra el préstamo si current_balance == 0.
    Inserta un movement 'adjustment' con monto 0 como marca de cierre.
    Actualiza el status del préstamo a 'closed'.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id, current_balance, status, folio FROM loans WHERE id = %s FOR UPDATE", (loan_id,))
        loan = cursor.fetchone()
        if not loan:
            cursor.close()
            conn.close()
            return {"error": f"No existe el préstamo {loan_id}."}

        if float(loan["current_balance"]) != 0.0:
            cursor.close()
            conn.close()
            return {"error": f"El préstamo {loan['folio']} no tiene saldo cero (saldo actual: {loan['current_balance']}). No puede cerrarse."}

        if loan["status"] == 'closed':
            cursor.close()
            conn.close()
            return {"error": f"El préstamo {loan['folio']} ya está cerrado."}

        cdate = datetime.strptime(close_date, "%Y-%m-%d").date() if close_date else datetime.now().date()

        # Marca de cierre
        cursor.execute("""
            INSERT INTO movements (
                loan_id, movement_type, amount, previous_balance, new_balance,
                movement_date, application_period, reference, note
            ) VALUES (%s, 'adjustment', 0, 0, 0, %s, %s, %s, %s)
            RETURNING id
        """, (loan_id, cdate, cdate.strftime("%Y-%m"), "CLOSE", note or "Cierre de préstamo"))
        mov = cursor.fetchone()

        # Actualizar status del préstamo
        cursor.execute("""
            UPDATE loans SET status = 'closed' WHERE id = %s
            RETURNING id, folio, status
        """, (loan_id,))
        updated_loan = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "movement_id": mov["id"],
            "loan": {
                "id": updated_loan["id"],
                "folio": updated_loan["folio"],
                "status": updated_loan["status"]
            },
            "message": f"Préstamo {updated_loan['folio']} cerrado exitosamente."
        }
    except Exception as e:
        return {"error": f"Error en Close_loan_if_zero: {str(e)}"}

@app.tool
def Get_pending_interest_payments_by_client_id(client_id: int) -> List[Dict[str, Any]]:
    """
    Obtiene los pagos de intereses pendientes para todos los préstamos de un cliente.
    Devuelve los resultados ordenados de menor a mayor por fecha de vencimiento.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.id AS statement_id, s.loan_id, l.folio, l.original_amount, l.current_balance,
                   s.period, s.interest_generated, s.interest_paid, s.due_date, s.status
            FROM statements s
            JOIN loans l ON s.loan_id = l.id
            WHERE l.client_id = %s
              AND s.status IN ('pending', 'partial')
              AND s.due_date >= CURRENT_DATE
            ORDER BY s.due_date ASC
        """, (client_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        pending_payments = []
        for row in rows:
            pending_interest = float(row["interest_generated"]) - float(row["interest_paid"])
            pending_payments.append({
                "statement_id": row["statement_id"],
                "loan_id": row["loan_id"],
                "folio": row["folio"],
                "original_amount": float(row["original_amount"]),
                "current_balance": float(row["current_balance"]),
                "period": row["period"],
                "interest_generated": float(row["interest_generated"]),
                "interest_paid": float(row["interest_paid"]),
                "pending_interest": pending_interest,
                "due_date": row["due_date"].strftime('%Y-%m-%d'),
                "status": row["status"]
            })
        return pending_payments
    except Exception as e:
        return [{"error": f"Error en Get_pending_interest_payments: {str(e)}"}]

@app.tool
def Generate_monthly_cutoff_for_period(period: str, due_days: int = 10) -> Dict[str, Any]:
    """
    Genera el corte mensual para TODOS los préstamos activos en el periodo especificado (YYYY-MM).
    - La fecha de corte se calcula automáticamente: mismo día que el start_date, pero con mes/año del periodo.
    - No genera corte si el periodo coincide con el mes del start_date.
    - Solo genera un corte por préstamo y periodo.
    - Retorna resumen de resultados.
    """
    try:
        # Validar formato del periodo
        try:
            cutoff_month = datetime.strptime(period, "%Y-%m")
        except ValueError:
            return {"error": "El periodo debe tener formato YYYY-MM."}

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener todos los préstamos activos
        cursor.execute("""
            SELECT id, client_id, original_amount, current_balance, interest_rate, start_date, status, folio
            FROM loans
            WHERE status = 'active'
        """)
        loans = cursor.fetchall()

        results = {
            "success": True,
            "period": period,
            "generated": 0,
            "skipped": 0,
            "errors": 0,
            "details": []
        }

        for loan in loans:
            loan_id = loan["id"]
            start_date = loan["start_date"]
            start_period = start_date.strftime("%Y-%m")
            folio = loan["folio"]

            # Saltar si el periodo es el mismo mes/año que el start_date
            if period == start_period:
                results["skipped"] += 1
                results["details"].append({
                    "loan_id": loan_id,
                    "folio": folio,
                    "status": "skipped",
                    "reason": "skipped_same_month_as_start"
                })
                continue

            # Calcular fecha de corte: mismo día que el start_date, pero en el mes/año del periodo
            start_day = start_date.day
            try:
                cutoff_dt = cutoff_month.replace(day=start_day).date()
            except ValueError:
                # Día fuera de rango, usar último día del mes
                next_month = cutoff_month.replace(day=28) + timedelta(days=4)
                last_day = (next_month - timedelta(days=next_month.day)).day
                cutoff_dt = cutoff_month.replace(day=last_day).date()

            # Fecha de vencimiento
            due_date = cutoff_dt + timedelta(days=due_days)

            # Verificar si ya existe statement para ese periodo
            cursor.execute("""
                SELECT id FROM statements WHERE loan_id = %s AND period = %s
            """, (loan_id, period))
            existing = cursor.fetchone()
            if existing:
                results["skipped"] += 1
                results["details"].append({
                    "loan_id": loan_id,
                    "folio": folio,
                    "status": "skipped",
                    "reason": "already_exists"
                })
                continue

            current_balance = float(loan["current_balance"])
            interest_rate = float(loan["interest_rate"])
            interest_generated = round(current_balance * (interest_rate / 100.0), 2)

            # Insertar movimiento de cargo de interés
            cursor.execute("""
                INSERT INTO movements (
                    loan_id, movement_type, amount, previous_balance, new_balance,
                    movement_date, application_period, reference, note
                )
                VALUES (%s, 'interest_charge', %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                loan_id,
                interest_generated,
                current_balance,
                current_balance,
                cutoff_dt,
                period,
                f"INT-{period}",
                "Cargo de interés mensual"
            ))
            movement_row = cursor.fetchone()

            # Insertar statement
            cursor.execute("""
                INSERT INTO statements (
                    loan_id, period, initial_balance, final_balance,
                    interest_generated, interest_paid, principal_paid,
                    cut_off_date, due_date, status
                )
                VALUES (%s, %s, %s, %s, %s, 0, 0, %s, %s, 'pending')
                RETURNING id, period, initial_balance, final_balance, interest_generated, 
                          cut_off_date, due_date, status
            """, (
                loan_id, period,
                current_balance,
                current_balance,
                interest_generated,
                cutoff_dt, due_date
            ))
            statement_row = cursor.fetchone()

            results["generated"] += 1
            results["details"].append({
                "loan_id": loan_id,
                "folio": folio,
                "status": "generated",
                "statement_id": statement_row["id"],
                "interest_generated": interest_generated
            })

        conn.commit()
        cursor.close()
        conn.close()
        return results

    except Exception as e:
        return {"error": f"Error en Generate_monthly_cutoff_for_period: {str(e)}"}

@app.tool
def Get_all_pending_interest_statements() -> List[Dict[str, Any]]:
    """
    Obtiene todos los estados de cuenta (statements) pendientes de pagar en el sistema.
    Devuelve los resultados ordenados de menor a mayor por fecha de vencimiento.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.id AS statement_id, s.loan_id, l.folio, l.original_amount, l.current_balance,
                   s.period, s.interest_generated, s.interest_paid, s.due_date, s.status,
                   l.client_id, c.name as client_name
            FROM statements s
            JOIN loans l ON s.loan_id = l.id
            JOIN clients c ON l.client_id = c.id
            WHERE s.status IN ('pending', 'partial')
            ORDER BY s.due_date ASC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        pending_statements = []
        for row in rows:
            pending_interest = float(row["interest_generated"]) - float(row["interest_paid"])
            pending_statements.append({
                "statement_id": row["statement_id"],
                "loan_id": row["loan_id"],
                "folio": row["folio"],
                "client_id": row["client_id"],
                "client_name": row["client_name"],
                "original_amount": float(row["original_amount"]),
                "current_balance": float(row["current_balance"]),
                "period": row["period"],
                "interest_generated": float(row["interest_generated"]),
                "interest_paid": float(row["interest_paid"]),
                "pending_interest": pending_interest,
                "due_date": row["due_date"].strftime('%Y-%m-%d'),
                "status": row["status"]
            })
        return pending_statements
    except Exception as e:
        return [{"error": f"Error en Get_all_pending_interest_statements: {str(e)}"}]

if __name__ == "__main__":
    app.run(transport="sse", host="0.0.0.0", port=3000)