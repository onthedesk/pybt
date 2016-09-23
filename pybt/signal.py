
import cython as cy
from copy import deepcopy

class Node(object):

    _price = cy.declare(cy.double)
    _value = cy.declare(cy.double)
    _weight = cy.declare(cy.double)
    _issec = cy.declare(cy.bint)
    _has_strat_children = cy.declare(cy.bint)

    def __init__(self, name, parent=None, children=None):

        self.name = name

        # strategy children helpers

        self._has_strat_children = False
        self._strat_children = []

        # if children is not none , we assume that we want to limit the
        # available children

        if children is not None:
            if isinstance(children, list):

                if all(isinstance(x, str) for x in children):
                    self._universe_tickers = children

                    children = {}

                else:
                    tmp = {}
                    ut = []

                    for c in children:

                        if type(c) == str:
                            tmp[c] = SecurityBase(c)
                            ut.append(c)
                        else:

                            tmp[c.name] = deepcopy(c)

                    children = tmp

                    self._universe_tickers = ut

        if parent is None:
            self.parent = self
            self.root = self

            self.integer_positions = True

        else:
            self.parent = parent
            self.root = parent.root
            parent._add_child(self)

        # default children


        if children is None
            children = {}
            self._universe_tickers = None

        self.children = children

        self._childrenv = list(children.values())

        for c in self._childrenv:
            c.parent = self
            c.root = self.root


        self.now = 0

        self.root.stale = False

        self._price = 0

        self._value = 0

        self._weight = 0

        self._issec = False

    def __getitem__(self, item):
        return self.children[item]

    def use_integer_positions(self, integer_positions):

        self.integer_positions = integer_positions

        for c in self._childrenv:
            c.use_interger_positions(integer_positions)

    @property
    def prices(self):

        raise NotImplementedError()

    @property
    def price(self):

        raise NotImplementedError()

    @property
    def value(self):

        if self.root.stale:
            self.root.update(self.root.now, None)

        return self._value

    @property
    def weight(self):

        if self.root.stale:
            self.root.update(self.root.now, None)

        return self._weight

class SecurityBase(Node):

    """
    Security Node. Used to define a security within a tree.
    A Security's has no children. It simply models an asset that can be bought
    or sold.

    Args:
        * name (str): Security name
        * multiplier (float): security multiplier - typically used for
            derivatives.

    Attributes:
        * name (str): Security name
        * parent (Security): Security parent
        * root (Security): Root node of the tree (topmost node)
        * now (datetime): Used when backtesting to store current date
        * stale (bool): Flag used to determine if Security is stale and need
            updating
        * prices (TimeSeries): Security prices.
        * price (float): last price
        * outlays (TimeSeries): Series of outlays. Positive outlays mean
            capital was allocated to security and security consumed that
            amount.  Negative outlays are the opposite. This can be useful for
            calculating turnover at the strategy level.
        * value (float): last value - basically position * price * multiplier
        * weight (float): weight in parent
        * full_name (str): Name including parents' names
        * members (list): Current Security + strategy's children
        * position (float): Current position (quantity).

    """

    _last_pos = cy.declare(cy.double)
    _position = cy.declare(cy.double)
    multiplier = cy.declare(cy.double)
    _prices_set = cy.declare(cy.bint)
    _needupdate = cy.declare(cy.bint)
    _outlay = cy.declare(cy.double)

    @cy.locals(multiplier=cy.double)
    def __init__(self, name, multiplier=1):
        Node.__init__(self, name, parent=None, children=None)
        self._value = 0
        self._price = 0
        self._weight = 0
        self._position = 0
        self.multiplier = multiplier

        # opt
        self._last_pos = 0
        self._issec = True
        self._needupdate = True
        self._outlay = 0

    @property
    def price(self):
        """
        Current price.
        """
        # if accessing and stale - update first
        if self._needupdate or self.now != self.parent.now:
            self.update(self.root.now)
        return self._price

    @property
    def prices(self):
        """
        TimeSeries of prices.
        """
        # if accessing and stale - update first
        if self._needupdate or self.now != self.parent.now:
            self.update(self.root.now)
        return self._prices.ix[:self.now]

    @property
    def values(self):
        """
        TimeSeries of values.
        """
        # if accessing and stale - update first
        if self._needupdate or self.now != self.parent.now:
            self.update(self.root.now)
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._values.ix[:self.now]

    @property
    def position(self):
        """
        Current position
        """
        # no stale check needed
        return self._position

    @property
    def positions(self):
        """
        TimeSeries of positions.
        """
        # if accessing and stale - update first
        if self._needupdate:
            self.update(self.root.now)
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._positions.ix[:self.now]

    @property
    def outlays(self):
        """
        TimeSeries of outlays. Positive outlays (buys) mean this security
        received and consumed capital (capital was allocated to it). Negative
        outlays are the opposite (the security close/sold, and returned capital
        to parent).
        """
        # if accessing and stale - update first
        return self._outlays.ix[:self.now]

    def setup(self, universe):
        """
        Setup Security with universe. Speeds up future runs.

        Args:
            * universe (DataFrame): DataFrame of prices with security's name as
                one of the columns.

        """
        # if we already have all the prices, we will store them to speed up
        # future udpates
        try:
            prices = universe[self.name]
        except KeyError:
            prices = None

        # setup internal data
        if prices is not None:
            self._prices = prices
            self.data = pd.DataFrame(index=universe.index,
                                     columns=['value', 'position'],
                                     data=0.0)
            self._prices_set = True
        else:
            self.data = pd.DataFrame(index=universe.index,
                                     columns=['price', 'value', 'position'])
            self._prices = self.data['price']
            self._prices_set = False

        self._values = self.data['value']
        self._positions = self.data['position']

        # add _outlay
        self.data['outlay'] = 0.
        self._outlays = self.data['outlay']

    @cy.locals(prc=cy.double)
    def update(self, date, data=None, inow=None):
        """
        Update security with a given date and optionally, some data.
        This will update price, value, weight, etc.
        """
        # filter for internal calls when position has not changed - nothing to
        # do. Internal calls (stale root calls) have None data. Also want to
        # make sure date has not changed, because then we do indeed want to
        # update.
        if date == self.now and self._last_pos == self._position:
            return

        if inow is None:
            if date == 0:
                inow = 0
            else:
                inow = self.data.index.get_loc(date)

        # date change - update price
        if date != self.now:
            # update now
            self.now = date

            if self._prices_set:
                self._price = self._prices.values[inow]
            # traditional data update
            elif data is not None:
                prc = data[self.name]
                self._price = prc
                self._prices.values[inow] = prc

        self._positions.values[inow] = self._position
        self._last_pos = self._position

        if np.isnan(self._price):
            if self._position == 0:
                self._value = 0
            else:
                raise Exception(
                    'Position is open (non-zero) and latest price is NaN '
                    'for security %s. Cannot update node value.' % self.name)
        else:
            self._value = self._position * self._price * self.multiplier

        self._values.values[inow] = self._value

        if self._weight == 0 and self._position == 0:
            self._needupdate = False

        # save outlay to outlays
        if self._outlay != 0:
            self._outlays.values[inow] = self._outlay
            # reset outlay back to 0
            self._outlay = 0

    @cy.locals(amount=cy.double, update=cy.bint, q=cy.double, outlay=cy.double)
    def allocate(self, amount, update=True):
        """
        This allocates capital to the Security. This is the method used to
        buy/sell the security.

        A given amount of shares will be determined on the current price, a
        commisison will be calculated based on the parent's commission fn, and
        any remaining capital will be passed back up  to parent as an
        adjustment.

        Args:
            * amount (float): Amount of adjustment.
            * update (bool): Force update?

        """

        # will need to update if this has been idle for a while...
        # update if needupdate or if now is stale
        # fetch parent's now since our now is stale
        if self._needupdate or self.now != self.parent.now:
            self.update(self.parent.now)

        # ignore 0 alloc
        # Note that if the price of security has dropped to zero, then it
        # should never be selected by SelectAll, SelectN etc. I.e. we should
        # not open the position at zero price. At the same time, we are able
        # to close it at zero price, because at that point amount=0.
        # Note also that we don't erase the position in an asset which price
        # has dropped to zero (though the weight will indeed be = 0)
        if amount == 0:
            return

        if self.parent is self or self.parent is None:
            raise Exception(
                'Cannot allocate capital to a parentless security')

        if self._price == 0 or np.isnan(self._price):
            raise Exception(
                'Cannot allocate capital to '
                '%s because price is %s as of %s'
                % (self.name, self._price, self.parent.now))

        # buy/sell
        # determine quantity - must also factor in commission
        # closing out?
        if amount == -self._value:
            q = -self._position
        else:
            q = amount / (self._price * self.multiplier)
            if self.integer_positions:
                if (self._position > 0) or ((self._position == 0) and (
                        amount > 0)):
                    # if we're going long or changing long position
                    q = math.floor(q)
                else:
                    # if we're going short or changing short position
                    q = math.ceil(q)

        # if q is 0 nothing to do
        if q == 0 or np.isnan(q):
            return

        # this security will need an update, even if pos is 0 (for example if
        # we close the positions, value and pos is 0, but still need to do that
        # last update)
        self._needupdate = True

        # adjust position & value
        self._position += q

        # calculate proper adjustment for parent
        # parent passed down amount so we want to pass
        # -outlay back up to parent to adjust for capital
        # used
        full_outlay, outlay, fee = self.outlay(q)

        # store outlay for future reference
        self._outlay += outlay

        # call parent
        self.parent.adjust(-full_outlay, update=update, flow=False, fee=fee)

    @cy.locals(q=cy.double, p=cy.double)
    def commission(self, q, p):
        """
        Calculates the commission (transaction fee) based on quantity and
        price.  Uses the parent's commission_fn.

        Args:
            * q (float): quantity
            * p (float): price

        """
        return self.parent.commission_fn(q, p)

    @cy.locals(q=cy.double)
    def outlay(self, q):
        """
        Determines the complete cash outlay (including commission) necessary
        given a quantity q.
        Second returning parameter is a commission itself.

        Args:
            * q (float): quantity

        """
        fee = self.commission(q, self._price * self.multiplier)
        outlay = q * self._price * self.multiplier
        return outlay + fee, outlay, fee

    def run(self):
        """
        Does nothing - securities have nothing to do on run.
        """
        pass