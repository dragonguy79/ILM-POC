create or replace PACKAGE BODY ILM_TEST AS

  PROCEDURE TEST_SQL_COUNT(EXPECTED_ROW_COUNT IN NUMBER, SQL_STMT IN VARCHAR2) AS
    ERROR_MSG VARCHAR2(500);
    actual_row_count NUMBER := 0;
  BEGIN

    -- actually executes test case
    EXECUTE IMMEDIATE SQL_STMT into actual_row_count;
    
    -- builds logging values
    IF actual_row_count != EXPECTED_ROW_COUNT THEN
      DBMS_OUTPUT.PUT_LINE('Assertion failed: Count assertion - expected row:'|| EXPECTED_ROW_COUNT || ', actual row:'||actual_row_count);
      DBMS_OUTPUT.PUT_LINE('Statement:'|| SQL_STMT);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Unexpected error in running statement: ' || SQL_STMT);
  END;
END ILM_TEST;