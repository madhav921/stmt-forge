from stmtforge.utils.scope_filter import is_irrelevant_filename, is_irrelevant_statement_text


def test_irrelevant_filename_patterns_are_filtered():
    assert is_irrelevant_filename("SBI_wealth_Daily_News_04062024.pdf", "sbi")
    assert is_irrelevant_filename("Statement_AUG2024_432681569.pdf", "icici")
    assert is_irrelevant_filename("My Debit Card Statement Jan.pdf", "unknown")


def test_credit_card_statement_filename_is_not_filtered():
    assert not is_irrelevant_filename("7411815536298014_15062024.pdf", "sbi")
    assert not is_irrelevant_filename("CreditCard_Statement_2026022215007394_21-02-2026.pdf", "federal")


def test_irrelevant_statement_text_is_filtered():
    assert is_irrelevant_statement_text("This is your Wealth Daily News update")
    assert is_irrelevant_statement_text("Debit card statement for your savings account")


def test_credit_card_statement_text_is_not_filtered():
    sample = """
    CREDIT CARD STATEMENT
    Statement Date: 21-02-2026
    Transactions
    19/02/2026 SWIGGY 399.00 Dr
    """
    assert not is_irrelevant_statement_text(sample)
