-- 1. FUNCIÓN: OBTENER PERÍODO ANTERIOR
-- Recibe un periodo (YYYYMM) y devuelve el mes anterior
CREATE OR REPLACE FUNCTION fn_periodo_anterior (
    p_anno_mes IN NUMBER
) RETURN NUMBER
IS
    v_fecha DATE;
BEGIN
    v_fecha := TO_DATE(TO_CHAR(p_anno_mes), 'YYYYMM');
    RETURN TO_NUMBER(TO_CHAR(ADD_MONTHS(v_fecha, -1), 'YYYYMM'));
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
/

-- 2. FUNCIÓN: CALCULAR MULTA EN PESOS
CREATE OR REPLACE FUNCTION fn_calcular_multa_pesos (
    p_cantidad_uf IN NUMBER,
    p_valor_uf    IN NUMBER
) RETURN NUMBER
IS
BEGIN
    RETURN ROUND(p_cantidad_uf * p_valor_uf);
END;
/

-- 3. PROCEDIMIENTO PRINCIPAL
CREATE OR REPLACE PROCEDURE pr_procesar_pago_cero (
    p_anno_mes_proceso IN NUMBER,
    p_valor_uf         IN NUMBER
)
IS
    -- Variables para manejo de periodos
    v_periodo_anterior     NUMBER;
    v_periodo_trasanterior NUMBER;
    
    -- Variables para chequeo de pagos
    v_pago_anterior_existe NUMBER;
    v_pago_trasant_existe  NUMBER;
    
    -- Variables para cálculos
    v_monto_multa          NUMBER;
    v_observacion          VARCHAR2(200);
    
    -- Cursor: Trae todos los gastos comunes del mes ACTUAL (el que estamos procesando)
    CURSOR c_gastos_actuales IS
        SELECT 
            gc.anno_mes_pcgc,
            gc.id_edif,
            gc.nro_depto,
            gc.fecha_pago_gc,
            e.nombre_edif,
            -- Datos Administrador
            ADM.numrun_adm || '-' || ADM.dvrun_adm AS run_admin,
            INITCAP(ADM.pnombre_adm || ' ' || ADM.appaterno_adm) AS nombre_admin,
            -- Datos Responsable
            R.numrun_rpgc || '-' || R.dvrun_rpgc AS run_resp,
            INITCAP(R.pnombre_rpgc || ' ' || R.snombre_rpgc || ' ' || R.appaterno_rpgc || ' ' || R.apmaterno_rpgc) AS nombre_resp
        FROM GASTO_COMUN gc
        JOIN EDIFICIO e ON gc.id_edif = e.id_edif
        JOIN ADMINISTRADOR adm ON e.numrun_adm = adm.numrun_adm
        JOIN RESPONSABLE_PAGO_GASTO_COMUN r ON gc.numrun_rpgc = r.numrun_rpgc
        WHERE gc.anno_mes_pcgc = p_anno_mes_proceso
        ORDER BY e.nombre_edif, gc.nro_depto;

BEGIN
    -- 1. Definir periodos históricos a revisar
    v_periodo_anterior     := fn_periodo_anterior(p_anno_mes_proceso);
    v_periodo_trasanterior := fn_periodo_anterior(v_periodo_anterior);

    -- 2. Limpiar tabla de paso para evitar duplicados en reprocesos
    DELETE FROM GASTO_COMUN_PAGO_CERO WHERE anno_mes_pcgc = p_anno_mes_proceso;

    -- 3. Recorrer cada departamento del periodo actual
    FOR r IN c_gastos_actuales LOOP
        
        -- A. Verificar si pagó el mes ANTERIOR
        -- Regla: Si NO existe registro en PAGO_GASTO_COMUN para el periodo anterior, es moroso.
        SELECT COUNT(*)
        INTO v_pago_anterior_existe
        FROM PAGO_GASTO_COMUN
        WHERE anno_mes_pcgc = v_periodo_anterior
          AND id_edif = r.id_edif
          AND nro_depto = r.nro_depto;

        -- Si NO pagó el mes anterior, procesamos la multa
        IF v_pago_anterior_existe = 0 THEN
            
            -- B. Verificar si pagó el mes TRASANTERIOR (Reincidencia)
            SELECT COUNT(*)
            INTO v_pago_trasant_existe
            FROM PAGO_GASTO_COMUN
            WHERE anno_mes_pcgc = v_periodo_trasanterior
              AND id_edif = r.id_edif
              AND nro_depto = r.nro_depto;

            -- C. Aplicar lógica de negocio (Multas y Observaciones)
            IF v_pago_trasant_existe = 0 THEN
                -- CASO: Debe más de un periodo (No pagó anterior NI trasanterior)
                -- Multa: 4 UF
                v_monto_multa := fn_calcular_multa_pesos(4, p_valor_uf);
                -- Observación con fecha de corte
                v_observacion := 'Se realizará el corte del combustible y agua a contar del ' || TO_CHAR(r.fecha_pago_gc, 'DD/MM/YYYY');
            ELSE
                -- CASO: Debe solo un periodo (No pagó anterior, pero sí el trasanterior)
                -- Multa: 2 UF
                v_monto_multa := fn_calcular_multa_pesos(2, p_valor_uf);
                -- Observación de aviso
                v_observacion := 'Se realizará el corte del combustible y agua';
            END IF;

            -- D. Insertar en tabla de reporte (GASTO_COMUN_PAGO_CERO)
            INSERT INTO GASTO_COMUN_PAGO_CERO (
                anno_mes_pcgc,
                id_edif,
                nombre_edif,
                run_administrador,
                nombre_admnistrador,
                nro_depto,
                run_responsable_pago_gc,
                nombre_responsable_pago_gc,
                valor_multa_pago_cero,
                observacion
            ) VALUES (
                p_anno_mes_proceso,
                r.id_edif,
                r.nombre_edif,
                r.run_admin,
                r.nombre_admin,
                r.nro_depto,
                r.run_resp,
                r.nombre_resp,
                v_monto_multa,
                v_observacion
            );

            -- E. Actualizar la multa en la tabla GASTO_COMUN del periodo actual
            UPDATE GASTO_COMUN
            SET multa_gc = v_monto_multa
            WHERE anno_mes_pcgc = p_anno_mes_proceso
              AND id_edif = r.id_edif
              AND nro_depto = r.nro_depto;

        END IF; -- Fin chequeo morosidad
        
    END LOOP;

    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20005, 'Error en proceso de pago cero: ' || SQLERRM);
END;
/

-- Ejecución
DECLARE
    v_periodo_actual NUMBER;
BEGIN
    -- Genera el periodo dinámicamente (Ej: '2026' || '05' = 202605)
    v_periodo_actual := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY') || '05');
    
    -- Ejecuta el procedimiento con el valor calculado
    pr_procesar_pago_cero(v_periodo_actual, 29509);
    
    DBMS_OUTPUT.PUT_LINE('Proceso finalizado para el periodo: ' || v_periodo_actual);
END;
/

-- Validación 1: Revisar tabla de reporte generada
SELECT * FROM GASTO_COMUN_PAGO_CERO 
ORDER BY nombre_edif, nro_depto;

-- Validación 2: Revisar actualización de multas en Gasto Común
SELECT anno_mes_pcgc, id_edif, nro_depto, fecha_desde_gc, fecha_hasta_gc, multa_gc
FROM GASTO_COMUN
WHERE anno_mes_pcgc = 202605
  AND multa_gc > 0
ORDER BY id_edif, nro_depto;

