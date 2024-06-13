from datetime import datetime, date

class PeriodicTransactionReport:
    document_id: str
    last: str
    first: str
    state_dst: str
    year: int
    filing_date: date

    def __init__(self, document_id: str, last: str, first: str, state_dst: str, year: int, filing_date: str) -> None:
        self.document_id = document_id
        self.last = last
        self.first = first
        self.state_dst = state_dst
        self.year = year
        self.filing_date = datetime.strptime(filing_date, '%m/%d/%Y').date()
    
    def to_dict(self):
        return {
            'document_id': self.document_id,
            'last': self.last,
            'first': self.first,
            'state_dst': self.state_dst,
            'year': self.year,
            'filing_date': self.filing_date
        }
        
    def __eq__(self, other):
        if isinstance(other, PeriodicTransactionReport):
            return self.document_id == other.document_id
        return False
    
    def __hash__(self):
        return hash(self.document_id)