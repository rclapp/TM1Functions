from TM1py.Services import TM1Service
from TM1py.Utils import Utils


def generate_attribute_df(tm1, dimension, attributeName = None):
    """
    :param tm1: TM1Py TM1 Connection
    :param dimension: dimension for retrieve
    :return: pandas data frame containing all attributes
    """
    # define MDX Query
    if attributeName is None:
        mdx = 'SELECT [}}ElementAttributes_{}].[}}ElementAttributes_{}].ALLMEMBERS ON 0,'\
            '[{}].[{}].ALLMEMBERS ON 1 ' \
            'FROM [}}ElementAttributes_{}]'\
            .format(dimension, dimension, dimension, dimension, dimension)
    else:
        mdx = 'SELECT {{[}}ElementAttributes_{}].[}}ElementAttributes_{}].[{}]}} ON 0, ' \
              '[{}].[{}].ALLMEMBERS ON 1 ' \
              'FROM [}}ElementAttributes_{}]'\
            .format(dimension, dimension, attributeName, dimension, dimension, dimension)
    # Get data from cube through MDX
    CubeData = tm1.cubes.cells.execute_mdx(mdx)
    # Build pandas DataFrame fram raw cellset data
    df = Utils.build_pandas_dataframe_from_cellset(CubeData)
    return df


def element_is_ancestor(tm1, dimension, hierarchy, ancestor, test_element):
    """
    :param tm1: TM1Py TM1 Connection
    :param dimension: Dimension for testing
    :param hierarchy: Hierarchy for testing
    :param ancestor:  Ancestor for testing
    :param test_element: Element to be tested for relationship to ancestor
    :return: Bool
    """

    children = tm1.dimensions.hierarchies.elements.get_members_under_consolidation(dimension, hierarchy, ancestor)
    if test_element in children:
        return True
    else:
        return False


