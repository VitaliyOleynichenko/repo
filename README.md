CREATE OR REPLACE PACKAGE PKG_TS_MNLZ2_TASK AS
  PROCEDURE LOAD_TASKS;
END PKG_TS_MNLZ2_TASK;
/



CREATE OR REPLACE PACKAGE BODY PKG_TS_MNLZ2_TASK AS

  PROCEDURE LOAD_TASKS IS
    ----------------------------------------------------------------------------
    -- Локальные переменные
    ----------------------------------------------------------------------------
    v_current_count  NUMBER;   -- сколько записей сейчас в T_TS_MNLZ2_TASK25
    v_needed         NUMBER;   -- сколько нужно добавить, чтобы довести до 25
    v_inserted       NUMBER := 0; -- сколько фактически добавили
    ----------------------------------------------------------------------------
    -- Переменные для "последней" записи
    v_last_id        V_TS_MNLZ2_TASK.ID%TYPE;
    v_last_id_seq    V_TS_MNLZ2_TASK.ID_SEQ%TYPE;
    v_last_datnz     V_TS_MNLZ2_TASK.DATNZ%TYPE;
    v_found_last     BOOLEAN := FALSE; -- нашли ли мы "последнюю" запись в курсоре
    ----------------------------------------------------------------------------
    
    ----------------------------------------------------------------------------
    -- Курсор, выбирающий данные из V_TS_MNLZ2_TASK с фильтром и сортировкой
    ----------------------------------------------------------------------------
    CURSOR c_source IS
      SELECT 
             ID,
             DATNZ,
             NNZ,
             OCHER,
             ID_SEQ
             -- при необходимости добавить остальные поля
        FROM V_TS_MNLZ2_TASK
       WHERE DATNZ >= SYSDATE
       ORDER BY DATNZ, NNZ, OCHER, ID_SEQ; 
       -- Если нужен иной порядок (например, убывающий), укажите явно DESC
    
    c_source_rec c_source%ROWTYPE;
    ----------------------------------------------------------------------------

  BEGIN
    ----------------------------------------------------------------------------
    -- 1) Проверяем, сколько записей уже в T_TS_MNLZ2_TASK25
    ----------------------------------------------------------------------------
    SELECT COUNT(*)
      INTO v_current_count
      FROM T_TS_MNLZ2_TASK25;
    
    IF v_current_count >= 20 THEN
      DBMS_OUTPUT.PUT_LINE('Записей уже 20 и более, добавлять не нужно.');
      RETURN;
    ELSE
      v_needed := 25 - v_current_count;
    END IF;
    
    ----------------------------------------------------------------------------
    -- 2) Считываем "последнюю" добавленную запись из T_TS_MNLZ2_LASTRECORD
    ----------------------------------------------------------------------------
    BEGIN
      SELECT ID,
             ID_SEQ,
             DATNZ
        INTO v_last_id,
             v_last_id_seq,
             v_last_datnz
        FROM T_TS_MNLZ2_LASTRECORD
       WHERE ROWNUM = 1;  -- предполагаем, что там ровно одна (или берём первую)
      
      -- если эта команда сработала, значит запись найдена
      v_found_last := FALSE; -- пока не нашли её в исходном курсоре
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Если таблица T_TS_MNLZ2_LASTRECORD пуста, начинаем "с самого начала"
        v_last_id     := NULL;
        v_last_id_seq := NULL;
        v_last_datnz  := NULL;
        v_found_last  := TRUE;  -- условно считаем, что "искать нечего", сразу вставляем
    END;
    
    ----------------------------------------------------------------------------
    -- 3) Перебираем записи из V_TS_MNLZ2_TASK курсором
    ----------------------------------------------------------------------------
    OPEN c_source;
    LOOP
      FETCH c_source INTO c_source_rec;
      EXIT WHEN c_source%NOTFOUND;
      
      ----------------------------------------------------------------------------
      -- Если мы ещё не нашли последнюю запись (v_found_last=FALSE), 
      -- то проверяем, совпадает ли текущая строка с последней записью.
      -- Как только найдём - переключим флаг, и на следующей итерации 
      -- уже будем потенциально добавлять новые строки.
      ----------------------------------------------------------------------------
      IF NOT v_found_last AND v_last_id IS NOT NULL THEN
        IF    c_source_rec.ID      = v_last_id
           AND c_source_rec.ID_SEQ = v_last_id_seq
           AND c_source_rec.DATNZ  = v_last_datnz
        THEN
          -- мы нашли ту самую "последнюю" добавленную
          v_found_last := TRUE;
          CONTINUE; -- идём дальше, чтобы начинать добавлять со следующей строки
        ELSE
          -- продолжаем искать
          CONTINUE;
        END IF;
      END IF;
      
      ----------------------------------------------------------------------------
      -- Если дошли сюда, значит либо:
      -- 1) У нас вообще не было "последней" записи (v_last_id IS NULL),
      --    и мы решили сразу добавлять, т.к. v_found_last = TRUE,
      -- 2) Или мы уже нашли последнюю запись и можем добавлять новые.
      ----------------------------------------------------------------------------
      
      ----------------------------------------------------------------------------
      -- 4) Проверяем, нет ли уже дубликата в T_TS_MNLZ2_TASK25
      ----------------------------------------------------------------------------
      DECLARE
        v_dup_count NUMBER;
      BEGIN
        SELECT COUNT(*)
          INTO v_dup_count
          FROM T_TS_MNLZ2_TASK25
         WHERE ID     = c_source_rec.ID
           AND ID_SEQ = c_source_rec.ID_SEQ
           AND DATNZ  = c_source_rec.DATNZ; 
           -- если ключевые поля другие, подставьте нужные
           
        IF v_dup_count = 0 THEN
          -- 5) Добавляем запись
          INSERT INTO T_TS_MNLZ2_TASK25
              (ID, DATNZ, NNZ, OCHER, ID_SEQ)
          VALUES (c_source_rec.ID,
                  c_source_rec.DATNZ,
                  c_source_rec.NNZ,
                  c_source_rec.OCHER,
                  c_source_rec.ID_SEQ);
          
          v_inserted := v_inserted + 1;
          
          -- Обновим "последнюю добавленную" на текущую
          v_last_id     := c_source_rec.ID;
          v_last_id_seq := c_source_rec.ID_SEQ;
          v_last_datnz  := c_source_rec.DATNZ;
          
          -- Если добавили всё, что нужно, выходим
          IF v_inserted >= v_needed THEN
            EXIT;
          END IF;
        END IF;
      END;
      
    END LOOP;
    CLOSE c_source;
    
    ----------------------------------------------------------------------------
    -- 6) Если что-то добавили, обновим (или вставим) запись в T_TS_MNLZ2_LASTRECORD
    ----------------------------------------------------------------------------
    IF v_inserted > 0 THEN
      -- Предположим, что в T_TS_MNLZ2_LASTRECORD должна храниться ровно одна запись.
      -- Можно сделать MERGE или UPDATE... 
      -- Ниже пример MERGE (если не найдёт, то вставит).
      MERGE INTO T_TS_MNLZ2_LASTRECORD LR
      USING (SELECT v_last_id     AS ID,
                    v_last_id_seq AS ID_SEQ,
                    v_last_datnz  AS DATNZ
             FROM DUAL) SRC
         ON (LR.ID = SRC.ID)  -- если ID - уникальный ключ
       WHEN MATCHED THEN
         UPDATE SET LR.ID_SEQ = SRC.ID_SEQ,
                    LR.DATNZ  = SRC.DATNZ
       WHEN NOT MATCHED THEN
         INSERT (ID, ID_SEQ, DATNZ)
         VALUES (SRC.ID, SRC.ID_SEQ, SRC.DATNZ);
      
      DBMS_OUTPUT.PUT_LINE('Добавлено ' || v_inserted || ' записей.');
    ELSE
      DBMS_OUTPUT.PUT_LINE('Новых уникальных записей не найдено.');
    END IF;
    
  END LOAD_TASKS;

END PKG_TS_MNLZ2_TASK;
/
