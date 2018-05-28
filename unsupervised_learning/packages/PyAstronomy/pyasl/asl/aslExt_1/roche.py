import numpy as np
from PyAstronomy.pyaC import pyaErrors as PE

def roche_qhin(x, y, z, q):
    """
    Normalized Roche potential
    
    The geometry is such that the more massive primary star is located at
    (x,y,z)=(0,0,0) and the secondary is located at (x,y,z)=(1,0,0). Therefore,
    the semi-major axis, a, is taken to be one (a=1).
    The output is a "normalized" Roche potential. To convert into physical units,
    the output has to be multiplied by -G(m1+m2)/(2*sma), where m1 and m2 are the
    masses of the stellar component, G is the gravitational constant, and sma is
    the semi-major axis. 
    
    From Sect. 4.3 of "An Introduction to Close Binary Stars" by R.W. Hilditch
    (Cambridge University Press 2001).
    
    Parameters
    ----------
    x,y,z : floats or arrays
        Cartesian coordinates (normalized by semi-major axis)
    q : float (0-1)
        Mass ratio (m2/m1)
    
    Returns
    -------
    nphi : float or array
        Normalized Roche potential at specified locations. 
    """
    # Hilditch p. 158
    y2, z2 = y**2, z**2
    yz2 = y2+ z2
    r1 = np.sqrt(x**2 + yz2)
    r2 = np.sqrt((x-1.)**2 + yz2)
    
    oq = 1.0/(1.0 + q)
    
    phin = 2.0*( oq/r1 + q*oq/r2 ) + (x - q*oq)**2 + y2
    return phin