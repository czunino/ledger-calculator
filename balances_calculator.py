from datetime import datetime, timedelta
from decimal import Decimal
from collections import namedtuple

# namedtuple definition to store advances data
AdvanceData = namedtuple("AdvanceData", ["event_date",          # datetime.date
                                         "original_amount",     # Decimal
                                         "current_balance"])    # Decimal


# namedtuple definition for the resulting data calculated by the BalancesCalculator
BalancesResult = namedtuple("BalancesResult", ["advances",                          # list of AdvanceData namedtuples
                                               "overall_advance_balance",           # Decimal
                                               "overall_interest_payable_balance",  # Decimal
                                               "overall_interest_paid",             # Decimal
                                               "overall_payments_for_future"])      # Decimal


class BalancesCalculator:
    """
    Main class that performs the balances calculation given an end date and several events in an interactive way.

    The class maintains some internal variables to perform the calculations, including a list of all the advances
    found in the process to keep track of their current balances and original amounts, a list of payments to be
    applied in the current day, and three numeric variables to store the Interest Payable Balance (IPB),
    the total interest paid and the payments for future advances.
    The main idea is based on collecting events for the same day, and when receives an event with a different
    (more recent) date, "close" the current calculation of the daily interests and start a new calculation with
    the next events. If the current and new event's date are not consecutive, it calculates the daily interests
    for each day until the new event's date.
    When calling the finish_calculation_and_get_results() it applies any missing daily interest calculation and
    returns the results in a BalancesResult namedtuple.

    NOTE1: the class requires that events to process are being received ordered by date
    NOTE2: several events for the same date are supported

    An example of usage of this class is:
        bc = BalancesCalculator(end_date=2021-10-30) to initialize a calculator object
        bc.process_event(2021-10-01, "advance", 100.5) to add an advance
        bc.process_event(2021-10-05, "payment", 120) to add a payment
        res = bc.finish_calculation_and_get_results() after all events where added finish calculation and get results

    """
    # constants used within the class
    ADVANCE = 'advance'
    PAYMENT = 'payment'
    DAILY_ACCRUED_INTEREST_CONSTANT = Decimal(0.00035)

    def __init__(self, end_date: datetime.date):
        """ Inits internal vars """
        self.end_date = end_date

        # list of all advances found during the process to keep track of their balances,
        # each item will be a dict with their dates, original_amount and current_balance
        self.advances = []

        # list of current payments amounts (decimals) to be process in the current day
        self.current_payments = []

        # last calculated date to keep track during the calculation
        self.last_calculated_date = None

        # IPB calculation up to self.last_calculated_date
        self.interest_payable_balance = Decimal(0)

        # interests paid calculation up to self.last_calculated_date
        self.interest_paid = Decimal(0)

        # payments for future advances up to self.last_calculated_date
        self.payments_for_future = Decimal(0)

    def process_event(self, event_date: datetime.date, event_type: str, amount: Decimal):
        """
        Main class function to receive and process events.
        Events are stored in internal vars until a next event with a new date is received, in which case the
        current day calculation is "closed" and the daily interests calculations are applied, to keep working next
        with the new event's date.

        :param event_date:  event's date, should be: self.last_calculated_date <= event_date <= self.end_date
        :param event_type:  event's type, should be "advance" or "payment"
        :param amount:      event's amount, should be > 0
        """
        # previous date calculation
        event_previous_date = event_date - timedelta(days=1)

        # for the first event, we initialize self.last_calculated_date with the previous date value
        if self.last_calculated_date is None:
            self.last_calculated_date = event_previous_date

        # verifies that events received are ordered in time
        assert event_date <= self.end_date, \
            f"received a {event_type} type event with date={event_date} greater than end_date={self.end_date}"
        assert event_date >= self.last_calculated_date, \
            f"received a {event_type} type event with date={event_date} lesser than previous event"

        # verifies that amount is greater than zero
        assert amount > 0, f"received a {event_type} event with date={event_date} with invalid amount={amount}"

        # the date of the new received event can be equal or greater than the last calculated date.
        # (We already verified that can't be lesser.)
        # - if is equal, is another event for the current calculation, so we apply it
        # - if is greater, we should first "close" the current day calculation and calculate the daily interests
        #  until the day before of the new event
        if event_date > self.last_calculated_date:
            self._calculate_daily_interests_until(stop_date=event_previous_date)

        # process the event
        if event_type == self.ADVANCE:
            self._process_advance(event_date, amount)
        elif event_type == self.PAYMENT:
            self._process_payment(amount)
        else:
            raise ValueError(f"invalid event_type={event_type} received")

    def finish_calculation_and_get_results(self) -> BalancesResult:
        """ Method to finish the calculations and return the resulting data """

        if self.last_calculated_date != self.end_date:
            self._calculate_daily_interests_until(stop_date=self.end_date)

        # creates a list of AdvanceData namedtuples that include all necessary advances data to be returned next
        advances_data = [AdvanceData(event_date=a["event_date"],
                                     original_amount=a["original_amount"],
                                     current_balance=a["current_balance"]) for a in self.advances]

        # creates and returns the final namedtuple
        return BalancesResult(advances=advances_data,
                              overall_advance_balance=self._get_current_advance_balance(),
                              overall_interest_payable_balance=self.interest_payable_balance,
                              overall_interest_paid=self.interest_paid,
                              overall_payments_for_future=self.payments_for_future)

    def _process_advance(self, event_date: datetime.date, amount: Decimal):
        """
        Method that process an advance adding it in self.advances list.
        The list contains dicts with these event's fields: event_date, original_amount and current_balance.
        First checks if there's some credit available for future advances, if so it applies it directly.
        """

        if self.payments_for_future == 0:
            # there's no credit for future advances, so the current balance is the total amount
            self.advances.append({"event_date": event_date,
                                  "original_amount": amount,
                                  "current_balance": amount})
        else:
            if amount <= self.payments_for_future:
                # there's enough credit to fulfill the advance, so current balance is 0 for this advance
                self.payments_for_future -= amount
                self.advances.append({"event_date": event_date,
                                      "original_amount": amount,
                                      "current_balance": Decimal(0)})
            else:
                # there's some credit to discount from the original amount
                remaining_amount = amount - self.payments_for_future
                self.payments_for_future = Decimal(0)
                self.advances.append({"event_date": event_date,
                                      "original_amount": amount,
                                      "current_balance": remaining_amount})

    def _process_payment(self, amount: Decimal):
        """ Method that process a payment adding it in self.current_payments list """
        self.current_payments.append(amount)

    def _calculate_daily_interests_until(self, stop_date: datetime.date):
        """
        Method to calculate the daily interests until a given stop_date.

        First applies all pending payments for the current day, if any, and then updates the IPB considering the
        current advance balance and the daily accrued interest.
        This IPB calculation is done for each day until the stop date calculation if finished.
        """
        # first applies the payments for the current day, if any
        self._apply_current_payments()

        if self.last_calculated_date is not None:
            # then, for each day until the stop date if finished, calculates the daily interests
            while self.last_calculated_date < stop_date:

                # get the current advance balance (after all advances and payments were applied for the current day)
                current_advance_balance = self._get_current_advance_balance()

                # calculates the daily accrued interest
                daily_accrued_interest = current_advance_balance * self.DAILY_ACCRUED_INTEREST_CONSTANT

                # updated the IPB and the last calculated date
                self.interest_payable_balance += daily_accrued_interest
                self.last_calculated_date += timedelta(days=1)

    def _apply_current_payments(self):
        """
        Method that applies the payments of the current day stored in self.current_payments, if any.

        The logic to apply each payment is as follows:
            - if there's already some future credit, then we add more future credit,
            - if not, we apply the payment over the IPB.
            - then, if we have remaining payment amount, we iterate over each advance (ordered by date, older first)
              and try to cancel them as much as possible
            - finally, if we could cancel all advances and we still have some remaining amount, it is stored
              for future advances.
        """
        for amount in self.current_payments:
            if self.payments_for_future > 0:
                # if there's some future credit, add more credit
                self.payments_for_future += amount
            else:
                # if not, applies payment over the IPB
                remaining_amount = self._apply_interest_payable_balance_payment(amount)

                # if we have a remaining amount, apply advances payment
                if remaining_amount > 0:
                    remaining_amount = self._apply_advances_payment(remaining_amount)

                # if we have a remaining amount we store it as payment for future advances
                if remaining_amount > 0:
                    self.payments_for_future = remaining_amount

        # finally resets the list to empty list for next calculation
        self.current_payments = []

    def _apply_interest_payable_balance_payment(self, amount: Decimal) -> Decimal:
        """
        Helper function that applies the IPB payment given the amount updating the self.interest_payable_balance
        and self.interest_paid values.
        It returns the remaining amount.
        """
        if self.interest_payable_balance > 0:
            if amount >= self.interest_payable_balance:
                self.interest_paid += self.interest_payable_balance
                amount -= self.interest_payable_balance
                self.interest_payable_balance = Decimal(0)
            else:
                self.interest_payable_balance -= amount
                self.interest_paid += self.interest_payable_balance
                amount = Decimal(0)

        return amount

    def _apply_advances_payment(self, amount: Decimal) -> Decimal:
        """
        Helper function that applies the advances payment given an amount.
        It loops and updates the self.advances list (that was created ordered by the advance dates) trying to cancel
        them as much as possible.
        It returns the remaining amount.
        """
        for advance in self.advances:
            if advance["current_balance"] > 0:
                if amount > advance["current_balance"]:
                    amount -= advance["current_balance"]
                    advance["current_balance"] = Decimal(0)
                else:
                    advance["current_balance"] -= amount
                    amount = Decimal(0)
                    break
        return amount

    def _get_current_advance_balance(self) -> Decimal:
        """
        Helper function to calculate the current advance balance aggregating over the current balance of each advance.
        """
        if len(self.advances) == 0:
            return Decimal(0)
        else:
            return sum([advance["current_balance"] for advance in self.advances])
